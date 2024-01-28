import geopandas as gpd
from sqlalchemy import create_engine, Column, BigInteger, text, MetaData, Float, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from geoalchemy2 import Geometry, WKBElement
from shapely import wkb

# Replace these values with your database connection parameters
DB_USER = ""
DB_PASSWORD = ""
DB_HOST = ""
DB_PORT = ""
DB_NAME = ""

def table_exists(engine, table_name, schema='public'):
    """
    Check if a table exists in the database.
    """
    metadata = MetaData()
    metadata.reflect(engine)
    return table_name in metadata.tables and metadata.tables[table_name].schema == schema

def load_shapefiles_to_db(shapefile_path, table_name):
    # Create a connection string to the database
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # Create an engine to connect to the database
    engine = create_engine(db_url)

    # Check if the table exists, and skip table creation if it does
    if not table_exists(engine, table_name):
        # Load Shapefile into GeoDataFrame
        gdf = gpd.read_file(shapefile_path)

        # Convert column names to lowercase
        gdf.columns = map(str.lower, gdf.columns)

        # Specify the schema
        schema = 'public'

        # Define the database table class using declarative_base
        Base = declarative_base()

        # Create a dictionary to store column definitions
        columns = {'id': Column(String, primary_key=True),  # Change to String
                   'geom': Column(Geometry('GEOMETRY'))}

        # Add necessary types for other columns
        for column_name in gdf.columns:
            if column_name not in ['geometry', 'id']:
                if gdf[column_name].dtype == 'int64':
                    columns[column_name] = Column(BigInteger)  # Change to BigInteger
                elif gdf[column_name].dtype == 'float64':
                    columns[column_name] = Column(Float)
                else:
                    columns[column_name] = Column(String)

        YourTable = type(table_name, (Base,), {
            '__tablename__': table_name,
            '__table_args__': {'schema': schema},
            **columns
        })

        # Create the table in the database
        YourTable.__table__.create(engine)

        # Create a session
        Session = sessionmaker(bind=engine)
        session = Session()

        # Populate the table with data
        for index, row in gdf.iterrows():
            # Convert Shapely geometry to WKBElement
            wkb_geometry = WKBElement(wkb.dumps(row['geometry']), srid=4326)
            data = YourTable(geom=wkb_geometry, **{col: row[col] for col in columns if col != 'geom'})
            session.add(data)

        # Commit the changes
        session.commit()

        # Create a spatial index
        index_sql = text(f'CREATE INDEX {table_name}_geom_idx ON {schema}.{table_name} USING gist(geom);')
        session.execute(index_sql)
        session.commit()
        session.close()
    else:
        print(f"Table {table_name} already exists. Skipping table creation.")

if __name__ == "__main__":
    # Replace these values with your Shapefile path and table name in the database
    shapefile_path = r""
    table_name = "nl_traffic_signs"

    load_shapefiles_to_db(shapefile_path, table_name)
