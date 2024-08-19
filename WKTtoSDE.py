import pandas as pd
import arcpy 
from arcpy import env
import cx_Oracle 
conn = cx_Oracle.connect('username/password@tnsname')
cursor = conn.cursor()
# Function to execute a query and return a DataFrame
def fetch_data_to_dataframe(query, conn):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        
        # Process the LOBs (CLOB/BLOB) before they are automatically closed
        processed_data = []
        for row in data:
            processed_row = []
            for col in row:
                if isinstance(col, cx_Oracle.LOB):
                    processed_row.append(col.read())  # Read the LOB data before closing
                else:
                    processed_row.append(col)
            processed_data.append(tuple(processed_row))

        columns = [col[0] for col in cursor.description]
        dataframe = pd.DataFrame(processed_data, columns=columns)
        return dataframe
    except Exception as e:
        print(f"Oracle'dan veri çekerken sorun oluştu: {e}")
        return None
    finally:
        cursor.close()
        conn.close()
        print("Oracle bağlantısı kapatıldı.")

# Example query to fetch data
query = "SELECT * FROM wktkatmani"
df = fetch_data_to_dataframe(query, conn)

# Display the dataframe if it was successfully created
if df is not None:
    print(df)

sde_connection = r"sdeconnectiondizini"
output_fc = f"{sde_connection}/sdegeomkatmani"
# Boş bir Feature Class oluşturma
spatial_ref = arcpy.SpatialReference("wkidyadaprjdosyayolu")  # WGS 84 (EPSG:4326) veya uygun başka bir koordinat sistemi
arcpy.CreateFeatureclass_management(sde_connection, "sdegeomkatmani", "pointpoligonyadapolyline", spatial_reference=spatial_ref)
# Tüm sütunları Feature Class'a ekleme
for column in df.columns:
    if column == 'WKT_GEOMETRY':  # Geometri sütunu zaten SHAPE@ ile işlenecek, atlıyoruz
        continue
    elif df[column].dtype == 'int64':
        arcpy.AddField_management(output_fc, column, "LONG")
    elif df[column].dtype == 'float64':
        arcpy.AddField_management(output_fc, column, "DOUBLE")
    else:
        arcpy.AddField_management(output_fc, column, "TEXT", field_length=255)

# Insert Cursor ile verileri Feature Class'a ekleme
with arcpy.da.InsertCursor(output_fc, ["SHAPE@"] + list(df.columns.drop('WKT_GEOMETRY'))) as cursor:
    for index, row in df.iterrows():
        # WKT'yi ArcPy geometrisine çevirme
        polyline = arcpy.FromWKT(row['WKT_GEOMETRY'], spatial_ref)
        # Verileri sıralı olarak cursor'a ekle
        cursor.insertRow([polyline] + [row[column] for column in df.columns if column != 'WKT_GEOMETRY'])

print("Veriler başarıyla SDE Feature Class'a export edildi.")
