# quickbooks-data
This is a Python based project meant to replicate &amp; generate QuickBooks records related to both sales and expense on a QuickBooks online Business.

## TASK

* We will be handling generating data for our Sales Table
* Also, we will be generating data for our ExpenseRecord, and SuppliesRecord Tables

## Assumptions
We will be assuming the following
* The data available in the database is for a single businesss.
* For any given QuickBooks table with no data but required during table generation will result to empty records

## How to install virtualenv:
1. Install virtualenv package on your pc

```bash
  $ sudo apt update
  $ sudo apt-get install virtualenv
```

2. Create virtualenv

```bash
  $ virtualenv venv 
```

3. Activate virtualenv
* For Windows

```bash
  $ venv\Scripts\activate
```

* For Linux

```bash
  $ source venv/bin/activate
```

## Install python dependencies

```bash
  $ pip install -r requirements.txt
```


## Install and configure Mysql Server

1. Run the command below to install mysql server.

```bash
  $ sudo apt update
  $ sudo apt install mysql-server
```

2.  Configure mysql

```bash
 $ sudo mysql_secure_installation
```

3. For further configurations, follow this link: [setup and configure mysql on ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-mysql-on-ubuntu-20-04)
4. Create a database schema with name '**link-unfurling**'
5. Fix mysql-client error
```bash
 $ sudo apt-get install python3-dev default-libmysqlclient-dev build-essential
```

## Setup Custom Environment file [.env]

1. Run the command below to create your local customer environment file.

```bash
  $ cp -b .env.example .env
```

2. Edit your environment file with your corresponding mysql db url string.
3. Add other required configuration files.

## Run generator

```bash
  $ python main.py
```

