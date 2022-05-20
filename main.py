#!/usr/bin/env python3

import io
import s3fs
import click
from halo import Halo

import sqlite3
import pandas


@click.group()
def main():
    """
    CLI to download CSV files from s3 and load them in an sqliteDB
    """
    pass

@main.command()
@click.option('--year', default="2021", help="Format YYYY")
@click.option('--month', default="04", help="Format MM")
@click.option('--host', default="s3://work-sample-mk")
@click.option('--filename', default="events.csv")
def getFile(year, month, host, filename):
    print(f"Args == {year}, {month}, {host}, {filename}") # DEBUG

    fs = s3fs.S3FileSystem(anon=True)

    with fs.open(f"{host}/{year}/{month}/{filename}", 'rb') as file:
        out = open(f"{year}_{month}.csv", 'wb')
        with Halo(text=f"Writing '{year}_{month}.csv' in progress", spinner='dots'):
            out.write(file.read())
        print("'{year}_{month}.csv' has been written")


@main.command()
@click.option('--dbname', default="Madkudu_events.db", help="Check if the DB Exist")
def testDB():

    conn = sqlite3.connect('Madkudu_events.db')
    cursor = conn.cursor()

    res = cursor.execute("SELECT sqlite_version();").fetchone()
    print(f"Version=={res[0]}")


@main.command()
@click.option('--year', default="2021", help="Format YYYY")
@click.option('--month', default="04", help="Format MM")
@click.option('--host', default="s3://work-sample-mk")
@click.option('--filename', default="events.csv")
def loadFile(year, month, host, filename):

    # Create S3 Object to connect without credential to the S3 Bucket
    fs = s3fs.S3FileSystem(anon=True)

    filelike = io.BytesIO()
    with fs.open(f"{host}/{year}/{month}/{filename}", 'rb') as file:
        with Halo(text=f"Downloading '../{year}/{month}/events.csv' in progress", spinner='dots'):
            filelike.write(file.read())
            # Reset the position of the filelike cursor
            filelike.seek(0)
        print("File downloaded")


        data = pandas.read_csv(filelike)
        # Data loaded in Pandas format

        conn = sqlite3.connect('Madkudu_events.db')
        cursor = conn.cursor()

        # Create the table
        cursor.execute("CREATE TABLE IF NOT EXISTS Events (id,timestamp,email,country,ip,uri,action,tags)")
        print("Table created")

        with Halo(text=f"Adding '../{year}/{month}/events.csv' to the DB in progress", spinner='dots'):
            data.to_sql('Events', conn, if_exists='append', index = False)
            print(f"'{year}_{month}.csv' has been added to the DB")

        rows = cursor.execute('''SELECT * FROM events''').fetchmany(20)
        print(f"rows=={rows}")

if __name__ == "__main__":
    main()
