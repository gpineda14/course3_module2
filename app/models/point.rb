class Point

  attr_accessor :longitude, :latitude

  def initialize(params)

    if params[:type]
      @longitude = params[:coordinates][0]
      @latitude = params[:coordinates][1]
    else
      @latitude = params[:lat]
      @longitude = params[:lng]
    end

  end

  def to_hash

    {
      "type": "Point",
      "coordinates": [@longitude, @latitude]
    }

  end

end
