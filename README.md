# **Madkudu**

Hello, welcome on the madkudu exercise.

---
## **Installation**

```
pip install -r requirement.txt
./main.py --help
```

---
## **Usage**

You will find 3 commands:

- **loadfile**      -- Download file and load it in the sqlite DB
- **downloadfile**  -- Just download the target file in local
- **testdb**        -- Check if the given database exist


#### -- **Loadfile** --
The main function `loadfile` will offer you a way to download a file from any s3 host.

By default, the downloaded file will be `s3://work-sample-mk/YEAR/MONTH/events.csv`

**YEAR** and **MONTH** are the values of the current date **minus 1 month**.

This will let you use the script to automate the download of file each 1st of month, by a `conjob` or in `Airflow` for exemple.

A feature will block a file if this one has already been inserted in the DB.



`main.py loadfile [OPTIONS]`

By default:
```
./main.py loadfile
```
You will get a *Madkudu_events.db* in your current folder



For futur evolutions, you can also pass those options:

**-y, --year** TEXT      Format YYYY::2022

**-m, --month** TEXT     Format MM::04

Will let you choose a specific date
```
./main.py loadfile -y2021 -m04
```

**-h, --host** TEXT      S3 path::s3://work-sample-mk
Will let you choose a specific s3 host

**-f, --filename** TEXT  File name::events.csv
Will let you choose a specific file name

**--dbname** TEXT        Chose the destination::Madkudu_events.db
Will let you specify the path and the name of you destination DB


The exercice was to get CSV data from an S3 host and provide the data in a usable format to be able to make request.
In this purpose, you would like to clean the data:

**-c, --clean**          Clean the data
```
./main.py loadfile -y2021 -m04 --clean
```

Currently, this option will merge duplicated rows and add a counter in a new column.
In addition, we could imagine to remove data outside de current month or add validation on columns values.

**-d, --debug**          Show error details
```
./main.py loadfile -y2021 -m04 -h 's3://badhost' -d
```
Allow the program to return stack trace for debug purpose. Else a simple error message is shown.


#### -- **Downloadfile** --

Give you a way to download the file without insert in a sqlite DB

`./main.py downloadfile [OPTION] [DEST] [SOURCE]`

```
./main.py downloadfile 'madkudu_event.csv' 's3://work-sample-mk/2021/04/events.csv'
```
You will get a fresh file `madkudu_event.csv` in your current directory

**DEST** - Specify the destination path and the filename

    Default: './{year}_{month}_{filename}'
    
**SOURCE** - Specify the source path and the filename

    Default: '{host}/{year}/{month}/{filename}'


#### -- **Testdb** --
Give you a simple way to check if the DB exist and is available

`main.py testdb [OPTIONS] [DBNAME]`

```
main.py testdb
```

**DBNAME** - Specify the full path of your targeted DB
    Default:'Madkudu_events.db'


---
## **Perspectives and evolutions**

So for now, the program is able to download a file from an s3 and to insert it in a sqliteDB

For an industrialization purpose, I would give you some suggestions:

### - On a ops perspective:
- Use Docker
- Call the script as purpose each 1st of the month with a cronjob or with Airflow for example
- Create a way to notice admin in case of fail. This may be done by an email or by using a trigger on the logs (see next point)

### - On a code perspective:
- Add a log function with level of log

Logs could be formated as followed:

`[Date Hours] [Unique Token] [Level Log] : Message`

Logs should offer the capacity to print the log and to save them in a specific file

We should also be able to set a log level by default to avoid to much verbose logs output

Levels could be (in order of priority): `[ERR_TRACE, WARN_TRACE, ERR, WARN, INFO, DEBUG, TRACE, LOG]`


### - On data perspective:
- Use postgreSQL instead of sqlite as a DB
- We could add more cleaning transformation.

I would also suggest to decoralate this point of the code and create an external config file


## **Development worflow**

I started by working on the file download.
I looked for a tool to get file on `S3` and so I used the lib `s3fs`

In order to offer a good experience with the tool, I implemented a CLI lib at the first step of the development.
I found the CLI `Click` which respected some conditions. The documentation is **clear** and **full**. The tool is complete with **lot of options** to customize easily the CLI. The use of the CLI is implemented by **decorator**. So its code is writen **outside the code logic** and keep the code **clean and simple**.

I also add a spinner for the long phases of download or write.

I still needed to offer a way to manage data in a proper form so I used `sqlite3` for its simplicity.

Before the insertion, I use a **file-like** python object : `io.BytesIO()`

This let to not write the file on the file system and to keep it in RAM after the download. I save some compute time but, in the futur, as files will become too big, this `optimisation` should be abandoned.

I also use **pandas** to insert the CSV into the sqlite database.

At this point, the big parts of the exercice have been done.


I restarted to work on it 2 days later, to add some options and flexibility.

So, I added a condition to avoid to insert two times the same file. I used a new table in the DB.

I also add Type on column during the table creation.

I added an error management and a debug option to keep the tool clean for a casual usage.

I also finetuned all the options of the CLI to respect the consign to have a tool `fully automated and able to recurrently
and periodically retrieving and processing the data without any external action`
That why, by default, the tool get the date of the previous month with the aim to run it periodically every 1st of the month.

And to open a way of the data transformation, I add a feature `--clean` to merge duplicated rows and add a column 'count' to keep the information.
In addition of this, we could imagine to remove rows outside the scope of the targeted month or to add validation on the data (email format, enum on `action` and `uri` columns, ...)


I hope you will enjoy my work and call me back for the next step.

Thank you
