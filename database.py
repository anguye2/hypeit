#!/usr/bin/python
import psycopg2
import io
import os, json, glob
from config import config
import sys
reload(sys)
sys.setdefaultencoding('utf8')

def create_table():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
        CREATE TABLE restaurant (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            rating FLOAT,
            price VARCHAR(225))
        """,
        """
        CREATE TABLE category (
            id SERIAL PRIMARY KEY,
            restaurant_id INTEGER NOT NULL,
            FOREIGN KEY (restaurant_id)
                REFERENCES restaurant (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            category VARCHAR(225))
        """,
        """
        CREATE TABLE known_for (
            id SERIAL PRIMARY KEY,
            restaurant_id INTEGER NOT NULL,
            FOREIGN KEY (restaurant_id)
                REFERENCES restaurant (id)
                ON UPDATE CASCADE ON DELETE CASCADE,
            name VARCHAR(225),
            value VARCHAR(225))
        """,
        """
        CREATE TABLE review (
            id SERIAL PRIMARY KEY,
            author VARCHAR(225) NOT NULL,
            rating INT,
            review VARCHAR,
            restaurant_id INTEGER NOT NULL,
            FOREIGN KEY (restaurant_id)
                REFERENCES restaurant (id)
                ON UPDATE CASCADE ON DELETE CASCADE)
        """)
    conn = None
    try:
        param = config()
        conn = psycopg2.connect(**param)
        cur = conn.cursor()

        for command in commands:
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        pass
    finally:
        if conn is not None:
            conn.close()

def insert_restaurant(cur, obj):
    # Insert into RESTAURANTS
    command = """
        INSERT INTO restaurant(name, rating, price)
        VALUES (%s, %s, %s)
        RETURNING id;
    """
    cur.execute(command, (obj['name'], obj['aggregateRating']['ratingValue'], obj['priceRange']))
    return cur.fetchone()[0]           

def insert_review(cur, restaurant_id, obj):
    command = """
        INSERT INTO review(author, rating, review, restaurant_id)
        VALUES (%s, %s, %s, %s);
    """
    reviews = obj['review']
    for i in reviews:
        cur.execute(command, (i['author'], i['reviewRating'], i['description'], restaurant_id))

def insert_category(cur, restaurant_id, file_category, obj):
    command = """
        INSERT INTO category(restaurant_id, category)
        VALUES (%s, %s);
    """
    cur.execute(command, (restaurant_id, file_category))  
    categories = obj['categories']
    for i in categories:
        category = str(i) 
        if category == file_category:
            pass
        else:
            cur.execute(command, (restaurant_id, category))  

def insert_known_for(cur, restaurant_id, obj):
    command = """
        INSERT INTO known_for(restaurant_id, name, value)
        VALUES (%s, %s, %s);
    """
    known_for = obj['known-for']
    for key, value in known_for.iteritems():
        # print key, value
        cur.execute(command, (restaurant_id, key, value))  

def insert_table():
    path_to_review = '/Users/anguyen/Desktop/hypeit/review/'
    for dirpath, dirnames, files in os.walk(path_to_review, topdown=False):
        #dirpath: pathname of current folder
        #dirnames: list of folders in the current folders
        #files: list of files in the folder      
        file_category = dirpath.rsplit("/",1)[1]
        restaurant_id = 0
        json_files = [pos_json for pos_json in os.listdir(dirpath) if pos_json.endswith('.json')]
        for index, js in enumerate(json_files): #index of files #js: name of each file          
            with open(os.path.join(dirpath, js)) as json_file:
                obj = json.load(json_file)               
                conn = None
                try:
                    param = config()
                    conn = psycopg2.connect(**param)
                    cur = conn.cursor()
                    restaurant_id = insert_restaurant(cur,obj)
                    insert_review(cur, restaurant_id, obj)
                    insert_category(cur, restaurant_id, file_category, obj)
                    insert_known_for(cur, restaurant_id, obj)
                    conn.commit()
                    cur.close()
                except (Exception, psycopg2.DatabaseError) as error:
                    print(error)
                finally:
                    if conn is not None:
                        conn.close()

def drop_table():
    conn = None
    rows_deleted = 0
    try:
        param = config()
        conn = psycopg2.connect(**param)
        cur = conn.cursor()
        cur.execute("DROP TABLE category")
        cur.execute("DROP TABLE known_for")
        cur.execute("DROP TABLE review")
        cur.execute("DROP TABLE restaurant")        
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

if __name__ == '__main__':   
    # drop_table()
    # create_table()
    insert_table()
    