This challenge was designed to assess your skills and knowledge in data analysis and visualization. As a BI Engineer, you will play a key role in extracting, transforming, and analyzing data to provide valuable insights.

In this challenge, you will face a situation where you need to deal with complex datasets, and your task will be to clean and model this data to create an efficient data warehouse environment. Additionally, you will be challenged to create interactive and intuitive visualizations to present the insights in a clear manner.

To succeed in this challenge, you will need to demonstrate proficiency in SQL and knowledge of business intelligence tools and technologies such as ETL (Extraction, Transformation, and Loading), data modeling, data analysis, and visualization. Furthermore, we expect you to demonstrate problem-solving skills, analytical thinking, and the ability to communicate insights effectively.

This challenge is an opportunity for you to showcase your technical skills and your ability to work with data efficiently and effectively. We look forward to seeing how you approach this challenge and the creative solutions you bring to the table.

Good luck and have fun!

# BI Engineer Task

There are four stages to this task:

**Data Gathering** : Unhash the data (`/data/bi_data.zip`),extract it and, more importantly, place it in a data warehouse like Snowflake or Big Query so you can use it for the rest of the tasks in this challenge.

**Data Cleaning** : Write SQL queries to answer the following questions using the data.

1. Can you identify some data quality issues within all the three sources that have been provided and cleaned before any further transformation or analysis is done?
2. Can you document the issues and assumptions made?


**Data Transformation** : Transform the raw data provided in order to create staging and summary tables as shown below. 

- Create a *User Journey* table with the following information

| Column_Name | Type | Description |
|-|-|-|
| user_id | INTEGER | Unique_Id of the user. It is the Primary Key of the table and only contains NOT NULL values |
| username | STRING | Username for the user |
| email | STRING | email_id of the user |
| first_name | STRING | first name of the user |
| last_name | STRING | last name of the user |
| address1 | STRING | address information of the user - obtained from the addresses column in the users.csv file |
| country | STRING | country information of the user - obtained from the addresses column in the users.csv file |
| city | STRING | city information of the user - obtained from the addresses column in the users.csv file |
| state | STRING | state information of the user - obtained from the addresses column in the users.csv file |
| zipcode | STRING | Zipcode information of the user - obtained from the addresses column in the users.csv file |
| total_orders | INTEGER | Total orders placed by the user |

- Create a *Transaction Journey* table with the following information

| Column_Name | Type | Description |
|-|-|-|
| item_id | STRING | Unique ID of the product. Not NULL column |
| user_id | INTEGER | Unique ID of the User. Not NULL column |
| product_name | STRING | Name of the product |
| product_category | STRING | Category to which the product belongs to |
| product_viewed | DATE TIME | Date time when the product was viewed by the user |
| product_added | DATE TIME | Date time when the product was added by the user |
| cart_viewed | DATE TIME | Date time when the cart was viewed by the user |
| checkout_started | DATE TIME | Date time when the checkout was started by the user |
| order_completed | DATE TIME | Date time when the order was completed by the user |
| discount | BOOLEAN | True or False column to indicate if a discount was applied to the product or not  |
| price | FLOAT | price of the product |

- Create a *Summary* table with the following information

| Column_Name | Type | Description |
|-|-|-|
| product_category | STRING | Category to which the product belongs to |
| total_products_viewed | INTEGER | Total product view events which happened within the category |
| total_products_added | INTEGER | Total product add events which happened within the category |
| total_cart_viewed | INTEGER | Total cart view events which happened within the category |
| total_checkout_started | INTEGER | Total checkout start events which happened within the category |
| total_orders_completed | INTEGER | Total order completed events which happened within the category |
| total_interactions | INTEGER | Total Interactions which happened within the category  |
| total_orders | INTEGER | Total Orders created for the category |
| total_customers_ordered | INTEGER | Total customers who ordered products of the particular category |
| total_revenue | FLOAT | Total revenue obtained for the particular category. |

**Data Analysis** : *Analyse the Data* - Based on the above tables created can you answer the following questions.

1. Which event has low transition rate and can you let us know the transition rate across each of the events?
2. What is the percentage of Cart abandonment across the store, where Cart abandonment consists of events_type (Added to cart and checkout started) but orders are not completed?
3. Find the average duration between checkout started and order completed and do you find any anomaly in the data?

**Data Visualisation** : *Visualise the Data* 

Create a dashboard to visualise the data and provide us with at least 3 interesting insights that you can draw from this data

## Data

The data can be found in the `/data/bi_data.zip` file.

While the data is synthetic, please consider it as being as close to reality as possible.

- **Users.csv**

| Column | Type | Description |
|-|-|-|
| id | INTEGER | Unique ID of the user and is a not null column |
| username | STRING | Username of the user |
| email | STRING | email id of the user |
| first_name | STRING | first name of the user |
| last_name | STRING | last name of the user |
| addresses | STRING | address information composed of address1,address2, country,city,state and zipcode |
| age | INTEGER | age of the user |
| gender | STRING | gender of the user |
| persona | STRING | persona to which the user belongs to |
| discount_persona | STRING | discount persona of the user |

- **Products.csv**

| Column | Type | Description |
|-|-|-|
| id | STRING | Unique ID of the product and is a not null column |
| url | STRING | url link to the product |
| name | STRING | name of the product |
| category | STRING | category to which the product belongs to |
| style | STRING | style to which the product belongs to |
| description | STRING | description of the product |
| price | FLOAT | price of the product |
| image | STRING | image of the product |
| gender_affinity | STRING | gender affinity of the product |
| current_stock | INTEGER | current stock availability of the product. Is always a positive value |

- **Interactions.csv**

| Column | Type | Description |
|-|-|-|
| Item_ID | STRING | Unique ID of the product and is a not null column |
| User_ID | INTEGER | Unique ID of the user and is a not null column |
| Event_Type | STRING | Different events such as ProductViewed,ProductAdded,CartViewed,CheckoutStarted,OrderCompleted happening within the website |
| Timestamp | INTEGER | Timestamp during which the events have occured |
| Discount | BOOLEAN | Discount if any has been applied on the orders completed |

**Useful Definitions**
- Revenue - It refers to the amount obtained from a product when an order completed event takes place
- Conversion rate - It is the sum of OrderCompleted events overs the Total_Events taken place
- Transition rate - It is the % of users moving from one event to the other. i.e. ProductViewed--> ProductAdded , ProductAdded --> CartViewed, CartViewed --> CheckoutStarted, CheckoutStarted -->OrderCompleted
- Cart abandonment - It refers to the number of users who have performed Add to cart and checkout started event but orders are not completed

All the best! Blow us away with your work!
