# queries_analysing

Finding and sorting the cccurencies of table columns in the daily queries.  
Change the table name after your needs (in main and in the query)


The **table_dict** file contains a dictionary of table column names from ** dl_dwh_dl** schema 
![image](https://user-images.githubusercontent.com/88441774/139666858-ee76641c-d260-4b56-bbf4-3ac9fc9ea8d8.png)


**Note**: This approach only gives you the information about, how often a field name is used in the queries but it doesn't give you the exact information about of which table it comes from. Example: If your query cookie_values from 2 tables and join them together ..., all the words 'cookie_value' are counted

