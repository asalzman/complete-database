import psycopg2
import unicodecsv as csv

conn = psycopg2.connect(database = "tweettext", user = "postgres", password = "1234")
cursor = conn.cursor()

f = open("/Users/michelleglewis/Downloads/ira_tweets_csv_hashed.csv", "rb")
'''
userinput = input('Enter name of file ')
f = open(userinput, "rb")
'''
df = csv.reader(f, encoding = 'utf-8')


labels = next(df)

list_of_types = ["bigint", "text", "text", "text", "text",  "text","text", "bigint", "bigint","bigint", "text","text", "text", "text", "text", "bigint", "text", "bigint", "text", "text", "bigint", "text", "text", "bigint", "bigint",  "bigint", "bigint", "text",  "text", "text", "text"]

cursor.execute("DROP TABLE table1;")
create_table_string = "CREATE TABLE IF NOT EXISTS table1("
for i in range(31):
    create_table_string += labels[i] + " " + list_of_types[i] + ", "
create_table_string = create_table_string[:-2]

create_table_string+= ");"

cursor.execute(create_table_string)
conn.commit()

list_of_names = labels
comma_separated_list = ""

for i in list_of_names:
    comma_separated_list = comma_separated_list + i + ", "

comma_separated_list = comma_separated_list[:-2]

insert_statement = ""

for i in range(31):
    insert_statement = insert_statement + "%s , "
insert_statement = insert_statement[:-2]
insertion_string = "INSERT INTO table1 VALUES (" + insert_statement + ")"

count = 0

for z in df:
    z = [x if x != "" else None for x in z]
    cursor.execute(insertion_string, z)
    count +=1
    if (count % 100000 == 0):
        print (count)
    
conn.commit()



