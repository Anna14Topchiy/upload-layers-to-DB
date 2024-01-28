import geopandas as gpd
from sqlalchemy import create_engine, Column, Integer, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry, WKBElement
from shapely import wkb

# Replace these values with your database connection parameters
DB_USER = "***"
DB_PASSWORD = "*****"
DB_HOST = "****"
DB_PORT = "****"
DB_NAME = "*****"

def load_shapefiles_to_db(shapefile_path, table_name):
    # Create a connection string to the database
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Create an engine to connect to the database
    engine = create_engine(db_url)

    # Load Shapefile into GeoDataFrame
    gdf = gpd.read_file(shapefile_path)

    # Specify the schema
    schema = 'public'

    # Define the database table class
    Base = declarative_base()

    class YourTable(Base):
        __tablename__ = table_name
        __table_args__ = {'schema': schema}

        id = Column(Integer, primary_key=True, autoincrement=True)
        geom = Column(Geometry('GEOMETRY'))

    # Create the table in the database
    Base.metadata.create_all(engine)

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Populate the table with data
    for index, row in gdf.iterrows():
        # Convert Shapely geometry to WKBElement
        wkb_geometry = WKBElement(wkb.dumps(row['geometry']), srid=4326)
        data = YourTable(geom=wkb_geometry)
        session.add(data)

    # Commit the changes to the database
    session.commit()

    # Create a spatial index
    index_sql = text(f'CREATE INDEX {table_name}_geom_idx ON {schema}.{table_name} USING gist(geom);')
    session.execute(index_sql)
    session.commit()
    session.close()

if __name__ == "__main__":
    # Replace these values with your Shapefile path and table name in the database
    shapefile_path = r"******"
    table_name = "******"

    load_shapefiles_to_db(shapefile_path, table_name)
