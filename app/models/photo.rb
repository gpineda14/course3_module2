class Photo
  include Mongoid::Document

  attr_accessor :id, :location
  attr_writer :contents

  def initialize(params=nil)

    @id = params[:_id].to_s if !params.nil? && !params[:_id].nil?
    @location = Point.new(params[:metadata][:location]) if !params.nil? && !params[:metadata].nil?
    @place = params[:metadata][:place] if !params.nil? && !params[:metadata].nil?

  end

  def persisted?

    !@id.nil?

  end

  def place

    if !@place.nil?
      Place.find(@place.to_s)
    end

  end

  def place=(id)

    if id.is_a?(String)
      @place = BSON::ObjectId.from_string(id)
    else
      @place = id
    end

  end

  def save

    if @place.is_a?(Place)
      @place = BSON::ObjectId.from_string(@place.id)
    end

    if !persisted?
      gps = EXIFR::JPEG.new(@contents).gps
      location = Point.new(:lng=>gps.longitude, :lat=>gps.latitude)
      @contents.rewind
      content = {}
      content[:content_type] = "image/jpeg"
      content[:metadata] = {
        :location => location.to_hash
      }
      gridfs = Mongo::Grid::File.new(@contents.read, content)
      @location = Point.new(location.to_hash)
      @id = mongo_client.database.fs.insert_one(gridfs).to_s
    else
      file = self.class.mongo_client.database.fs.find(:_id=>BSON::ObjectId.from_string(@id)).first
      file[:metadata][:location] = @location.to_hash
      file[:metadata][:place] = @place
      self.class.mongo_client.database.fs.find(:_id=>BSON::ObjectId.from_string(@id)).update_one(file)
    end

  end

  def self.mongo_client

    Mongoid::Clients.default

  end

  def self.all(offset=0, limit=nil)

    if !limit.nil?
      mongo_client.database.fs.find.skip(offset).limit(limit).map { |doc| Photo.new(doc) }
    else
      mongo_client.database.fs.find.skip(offset).map { |doc| Photo.new(doc) }
    end

  end

  def self.find(id)

    photo = mongo_client.database.fs.find(:_id=> BSON::ObjectId.from_string(id)).first

    if !photo.nil?
      file = Photo.new(photo)
    else
      nil
    end

  end

  def contents

    file = self.class.mongo_client.database.fs.find_one(:_id=>BSON::ObjectId.from_string(@id))

    if file
      buffer = ''
      file.chunks.reduce([]) do |x, chunk|
        buffer << chunk.data.data
      end
      return buffer
    end

  end

  def destroy

    self.class.mongo_client.database.fs.find(:_id=>BSON::ObjectId.from_string(@id)).delete_one

  end

  def find_nearest_place_id(max_distance)

    Place.near(@location.to_hash, max_distance).limit(1).projection({:_id=>1}).first[:_id]

  end

  def self.find_photos_for_place(place_id)

    place_id = place_id.is_a?(String) ? BSON::ObjectId.from_string(place_id) : place_id


    mongo_client.database.fs.find("metadata.place": place_id)

  end

end
