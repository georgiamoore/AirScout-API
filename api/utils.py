import psycopg2
import psycopg2.extras as extras

def convert_df_to_db_format(df, conn, cursor, table_name, renamed_cols):
    df = df.rename(columns = renamed_cols)
    # removing columns that don't exist in db
    cursor.execute("SELECT * FROM %s LIMIT 0" % (table_name,))
    db_cols = [desc[0] for desc in cursor.description]
    df = df[df.columns.intersection(db_cols)]
    
    # convert df to list of tuples for bulk insert to db
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ', '.join(f'"{c}"' for c in df.columns.tolist())
    query  = "INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING" % (table_name, cols)

    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        conn.rollback()
        cursor.close()
        # TODO fix error return format -> use Flask response
        return("Error: %s" % error)

    cursor.close()
    return "Sensor readings inserted successfully."
