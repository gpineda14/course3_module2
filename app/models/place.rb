class Place
  include ActiveModel::Model
  include Mongoid::Document

  attr_accessor :id, :formatted_address, :location, :address_components

  def initialize(params)

    @id = params[:_id].to_s
    @address_components = []
    if params[:address_components]
      params[:address_components].each do |address|
        @address_components << AddressComponent.new(address)
      end
    end
    @formatted_address = params[:formatted_address]
    @location = Point.new(params[:geometry][:geolocation])

  end

  def persisted?

    !@id.nil?

  end

  def self.mongo_client

    Mongoid::Clients.default

  end

  def self.collection

    self.mongo_client[:places]

  end

  def self.load_all(json)

    file = JSON.parse(json.read)
    collection.insert_many(file)

  end

  def self.find_by_short_name(name)

    collection.find({"address_components.short_name": name})

  end

  def self.to_places(places)

    locations = []
    places.each do |address|
      locations << Place.new(address)
    end
    return locations

  end

  def self.find(id)

    id_string = BSON::ObjectId.from_string(id)
    query = collection.find(:_id=> id_string).first
    if !query.nil?
      Place.new(query)
    else
      nil
    end

  end

  def self.all(offset=0, limit=nil)

    if !limit.nil?
      files = collection.find.skip(offset).limit(limit)
    else
      files = collection.find.skip(offset)
    end
    coll = []
    files.each do |file|
      coll << Place.new(file)
    end
    return coll

  end

  def destroy

    self.class.collection.find(:_id=>BSON::ObjectId.from_string(@id)).delete_one

  end

  def self.get_address_components(sort=nil, offset=0, limit=nil)

    if sort.nil? && limit.nil?

     collection.find.aggregate([

       {:$unwind=> '$address_components'},
       {:$project => {_id: 1, address_components: 1, formatted_address: 1, :geometry=>{:geolocation=> 1}}},
       {:$skip=> offset}

       ])

    elsif sort.nil? && !limit.nil?

      collection.find.aggregate([

        {:$unwind=> '$address_components'},
        {:$project => {_id: 1, address_components: 1, formatted_address: 1, :geometry=>{:geolocation=> 1}}},
        {:$skip=> offset},
        {:$limit=> limit}

        ])

    elsif limit.nil? && !sort.nil?

      collection.find.aggregate([

        {:$unwind=> '$address_components'},
        {:$project => {_id: 1, address_components: 1, formatted_address: 1, :geometry=>{:geolocation=> 1}}},
        {:$sort=> sort},
        {:$skip=> offset},

        ])

    else

      collection.find.aggregate([

        {:$unwind=> '$address_components'},
        {:$project => {_id: 1, address_components: 1, formatted_address: 1, :geolocation=>{:geolocation=> 1}}},
        {:$sort=> sort},
        {:$skip=> offset},
        {:$limit=> limit}

        ])

    end

  end

  def self.get_country_names

    collection.find.aggregate([

      {:$unwind=> '$address_components'},
      {:$project=> {_id: 0, :address_components=> {:long_name=> 1, :types=> 1}}},
      {:$match=> {"address_components.types": "country"}},
      {:$group=> {:_id=> '$address_components.long_name', :count=>{:$sum=> 1}}}
      ]).to_a.map {|country| country[:_id]}

  end

  def self.find_ids_by_country_code(country_code)

    collection.find.aggregate([

      {:$match=> {"address_components.short_name": country_code, "address_components.types": "country"}},
      {:$project=> {_id: 1}}
      ]).map {|doc| doc[:_id].to_s}

  end

  def self.create_indexes

    collection.indexes.create_one(
    {'geometry.geolocation': Mongo::Index::GEO2DSPHERE}
    )

  end

  def self.remove_indexes

    collection.indexes.drop_one(
    'geometry.geolocation_2dsphere'
    )

  end

  def self.near(point, max_meters=nil)

    if !max_meters.nil?
      collection.find('geometry.geolocation':
        {:$near=> {
          :$geometry=> point.to_hash,
          :$maxDistance=> max_meters
        }})
    else
      collection.find('geometry.geolocation':
        {:$near=> {
          :$geometry=> point.to_hash
        }})
    end

  end

  def near(max_meters=nil)

    Place.to_places(Place.near(@location.to_hash, max_meters))

  end

  def photos(offset=0, limit=nil)

    if !limit.nil?
      self.mongo_client.database.fs.find("metadata.place": BSON::ObjectId.from_string(@id)).skip(offset).limit(limit).map {|photo| Photo.new(photo)}
    else
      Photo.mongo_client.database.fs.find("metadata.place": BSON::ObjectId.from_string(@id)).skip(offset).map {|photo| Photo.new(photo)}
    end

  end

end
