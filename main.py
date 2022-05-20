#!/usr/bin/env python3


import s3fs
import click

@click.group()
def main():
    """
    CLI to download CSV files from s3 and load them in an useful DB format
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

    with fs.open(f"{host}/{year}/{month}/{filename}", 'rb') as f:
        out = open(f"{year}_{month}.csv", 'wb')
        out.write(f.read())


if __name__ == "__main__":
    main()
