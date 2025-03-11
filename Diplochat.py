import streamlit as st  
import streamlit_authenticator as stauth  
from streamlit_feedback import streamlit_feedback
import yaml
from yaml.loader import SafeLoader
import pyodbc  
import pandas as pd  
from datetime import datetime  
from openai import AzureOpenAI 
import warnings 
import time 
import re
from datetime import datetime
import pytz
import base64
from io import BytesIO
import matplotlib.pyplot as plt
import os
import numpy as np
import tiktoken
from pyluach import dates  
import bcrypt  
import subprocess

# Test
# Suppress all warnings  
warnings.filterwarnings('ignore')   

# from st_pages import hide_pages

db_password = os.getenv('DB_PASSWORD')

conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                server='diplomat-analytics-server.database.windows.net',  
                database='Diplochat-DB',  
                uid='analyticsadmin', pwd=db_password)  

user_query = """SELECT * FROM [dbo].[DW_DIM_USERS]"""
user_df = pd.read_sql_query(user_query, conn)

# Initialize the YAML structure  
yaml_data = {  
    'cookie': {  
        'expiry_days': 30,  
        'key': "some_signature_key",  # Replace with your actual signature key  
        'name': "auth_cookie"  # Replace with your desired cookie name  
    },  
    'credentials': {  
        'usernames': {}  # Initialize usernames as an empty dictionary  
    },  
    'pre-authorized': {  
        'emails': [  
            'ryonatan6@gmail.com'  # Add more emails as needed  
        ]  
    }  
}   
  
# Populate the usernames in the YAML structure from the DataFrame  
for index, row in user_df.iterrows():  
    username = row['username']  
    yaml_data['credentials']['usernames'][username] = {  
        "email": row['email'],  
        "failed_login_attempts": row['failed_login_attempts'],  
        "logged_in": row['logged_in'],  
        "name": row['name'],  
        "password": row['password']  # Ensure you handle the passwords securely  
    }  
  
# Convert the dictionary to YAML  
# config = yaml.dump(yaml_data, sort_keys=False)  
config = yaml_data

authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days'],
    config['pre-authorized']
)

st.title('Diplochat')

# Clear the cache on app start or refresh
if 'cache_cleared' not in st.session_state:
    st.cache_data.clear()
    st.session_state.cache_cleared = True

# Login widget  
authentication_status = authenticator.login()  

# # Button to show the form
# if st.button("Sign Up"):
#     # Create a form for user input
#     with st.form(key='signup_form'):
#         email = st.text_input("Email Address")
#         full_name = st.text_input("Full Name")

#         submit_button = st.form_submit_button(label='Sign Up')

#         if submit_button:
#             # Display the entered information
#             st.success("You have signed up successfully!")
#             st.write("Email Address:", email)
#             st.write("Full Name:", full_name)



# hide_pages([ "Map"])

# Adjusted authentication status handling  
if st.session_state['authentication_status']:  

    
    # st.sidebar.markdown("![](logo_2.png)")
    # st.sidebar.image("logo_2.png", use_column_width=True)
    st.sidebar.markdown("![](https://projects.telem-hit.net/2022/Diplomania_OfirShachar/about/styles/diplomatlogo.png)")
    authenticator.logout(location = 'sidebar')  # Add logout functionality  
    st.write(f'Welcome *{st.session_state["name"]}*')  # Display welcome message  


    # sales org
    if 'sales_org' not in st.session_state:
            st.session_state.sales_org = "DIL"  # default value

    sales_org = st.sidebar.radio(
        "Choose Sales Organization:", 
        ["DIL", "DGE", "DSA", "DNZ", "DDC"], 
        index=0 if st.session_state.sales_org == "DIL" else 1 if st.session_state.sales_org == "DGE" else 2 if st.session_state.sales_org == "DSA" else 3 if st.session_state.sales_org == "DNZ" else 4
    )    

    so_dict = dict(zip(["DIL", "DGE", "DSA", "DNZ", "DDC"],["1000","5000","8000","NZ00","DDC"]))

    # Check if the resolution type has changed and rerun/cache if it has
    if sales_org != st.session_state.sales_org:
        st.session_state.sales_org = sales_org


    # weekly/ monthly
    if 'resolution_type' not in st.session_state:
        st.session_state.resolution_type = "Monthly"  # default value


    
    
    # Sidebar radio button for choosing resolution type
    selected_resolution = st.sidebar.radio("Choose resolution:", ["Monthly", "Weekly"], index=0 if st.session_state.resolution_type == "Monthly" else 1)

    # Check if the resolution type has changed and rerun/cache if it has
    if selected_resolution != st.session_state.resolution_type:
        st.session_state.resolution_type = selected_resolution


    if 'chp_or_invoices' not in st.session_state:
            st.session_state.chp_or_invoices = "Invoices"  # default value

    # Sidebar radio button for choosing chp or invoices 
    chp_or_invoices = st.sidebar.radio("Choose data source:", ["Invoices", "CHP"], index=0 if st.session_state.chp_or_invoices == "Invoices" else 1)

    # Check if the resolution type has changed and rerun/cache if it has
    if chp_or_invoices != st.session_state.chp_or_invoices:
        st.session_state.chp_or_invoices = chp_or_invoices

    

    admin_list = ['Yonatan Rabinovich','Avi Tuval']
    # admin_list = ['Yonatan Rabinovich']
   
    # Initialize session state email
    if 'email' not in st.session_state:
        st.session_state.email = "" 
    # Initialize session state name
    if 'full_name' not in st.session_state:
        st.session_state.full_name = "" 

    def user_signup(full_name,email):
        if email in user_df.email.values:
            st.toast(f"❌ User {full_name} already in the system with mail: {email}!")
        elif '@' not in email or '.co' not in email:
            st.toast(f"❌ {email} not a valid email!")
        else:

            conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                    server='diplomat-analytics-server.database.windows.net',  
                    database='Diplochat-DB',  
                    uid='analyticsadmin', pwd=db_password) 
            # Insert log data into the AI_LOG table  
            insert_query = """  
            INSERT INTO DW_DIM_USERS (username, email, failed_login_attempts, logged_in, name, password)  
            VALUES (?, ?, ?, ?, ?, ?)  
            """  

            # username
            username = email.split('@')[0]
            password = email.split('@')[0]+''.join(str(i+1) for i in range(len(email.split('@')[0])))+'!'
            password = password.capitalize()
            hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()) 
            
            log_session = [username,email,0,0,full_name,hashed_password.decode('utf-8')]

            cursor = conn.cursor()
            cursor.execute(insert_query, log_session)
            


            conn.commit()  
            cursor.close()
            st.toast(f"✔️ User {full_name} signed up successfully with email: {email}!")
	
    # Check if the current user is an admin
    if st.session_state.get("name") in admin_list:
        # Sidebar for sign-up
        with st.sidebar:
            # Button to show the form
            with st.expander('Sign Up Form'):
                # Create a form for user input in the sidebar
                # with st.form(key='signup_form'):
                email = st.text_input("Email Address")
                full_name = st.text_area("Full Name")
                if st.button('Sign Up'):
			
                    user_signup(full_name,email)
                    # st.toast(f"✔️ User {full_name} signed up successfully with email: {email}!")

                    # Submit button, passing user's full name to the signup function
                    # submit_button = st.form_submit_button(label='Sign Up', on_click=user_signup, args=(full_name,email))
                    # submit_button = st.form_submit_button(label='Sign Up', on_click=user_signup)

    
        # Create an expander in the sidebar
        with st.sidebar.expander("What's the psd?"):
            # Use the text_area to take input from the user
            user_input = st.text_area("Enter email:")

            # Check if the user has entered any text
            if user_input:
                password = user_input.split('@')[0]+''.join(str(i+1) for i in range(len(user_input.split('@')[0])))+'!'
                password = password.capitalize()
                # Display a message incorporating the user input
                st.sidebar.write(f"The psd is: {password}")
            else:
                st.sidebar.write("Please enter A user email")
    #####################
    # diplochat analyst #
    #####################


    sys_msg = """  
    You are an AI Data Analyst assistant for DIPLOMAT DISTRIBUTORS (1968) LTD, and you are coding in Python. 
    The following datasets are already loaded in your Python IDE:  
    
    1. **DW_FACT_STORENEXT_BY_INDUSTRIES_SALES** (`stnx_sales`)  
    - **Description**: This dataset provides daily sales figures by item across different market segments.  
    - **Columns**:  
        - `Day`: Date (datetime).  
        - `Barcode`: Item identifier.  
        - `Format_Name`: Market segmentation.  
        - `Sales_NIS`: Sales amount in NIS.  
        - `Sales_Units`: Quantity sold.  
        - `Price_Per_Unit`: Daily price per unit.  
    - **Note**: Filter the data for the date range between 2023-12-31 and 2024-9-1.  
    
    2. **DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS** (`stnx_items`)  
    - **Description**: This is a dimension table containing attributes of items.  
    - **Columns**:  
        - `Barcode`: Item identifier.  
        - `Item_Name`: Name of the item.  
        - `Category_Name`: Name of the category.  
        - `Sub_Category_Name`: Name of the subcategory.  
        - `Brand_Name`: Name of the brand.  
        - `Sub_Brand_Name`: Name of the sub-brand.  
        - `Supplier_Name`: Name of the supplier.     
    
    3. **DW_CHP_AGGR** (`chp`)  
    - **Description**: This fact table records daily snack prices by barcode and chain, including promotions.  
    - **Columns**:
        - `DATE`: Date (datetime).    
        - `BARCODE`: Item identifier.  
        - `CHAIN`: Name of the supermarket chain.
        - `AVG_PRICE`: Base price accross the stores in the chain.
        - `AVG_SELLOUT_PRICE`: Promotional price accross the stores in the chain, if null or missing then the no promotion is currently applies, address the current price.    
        - `NUMBER_OF_STORES`: Number of stores in the chain that reported containing this barcode.  
        - `SELLOUT_DESCRIPTION`: Hebrew description of sales promotions, if null or missing then the no promotion is currently applies.  
    - **Note**: to check barcodes attributes, connect this table with stnx_items 'Barcode' and get the relevant info, data here is from '2023-12-31' to '2024-09-01'.  

    4. **DATE_HOLIAY_DATA** ('dt_df')
    - **Description**: This fact table records daily holiday data and hebrew dates.  
    - **Columns**:
        - `DATE`: Date (datetime).    
        - 'HEBREW_DATE': Hebrew date as string.
        - 'HOLIDAY': the name of the holiday or null if no holiday is on that date (string).
        - **Note**: this data is from a python process involving a package of hebrew dates and holidays. 

    5. **AGGR_MONTHLY_DW_INVOICES** ('inv_df'):
        - **Description**: This fact table records Diplomat's invoice data.
        - **Columns**:
            - `DATE`: Date (datetime). 
            - `SALES_ORGANIZATION_CODE`: The id of Diplomat's buisness unit, values: '1000' - Israel, '5000' - Georgia, '8000' - South Africa, 'NZ00' - New Zeeland.
            - `MATERIAL_CODE`: The id of Diplomat's items.
            - `INDUSTRY_CODE`:  The id of Diplomat's different industries that relate to their customers.
            - 'CUSTOMER_CODE': The id of the exact customers.
            - 'Gross': The gross sales.
            - 'Net': The net sales.
            - 'Net VAT': The net sales with tax.
            - 'Gross VAT': The gross sales with tax.
            - 'Units': the number of units.
            - **Note**: This data relates to the sell in of diplomat and needs the material barcode from the material table to connect to external data like chp and others,
                        And if you asked about diplomat which is not in israel , use Dollar (multiple the Answer by Dollar value and put sign)

        
    6. **DW_DIM_CUSTOMERS** ('customer_df'):
        - **Description**: The customers's information.
            - 'CUSTOMER_CODE': The id of the exact customers (primary key).
            - 'CUSTOMER':  Customer name.
            - 'CITY':  City of the customer.
            - 'CUSTOMER_ADDRESS':  Adress of the customer.
            - 'CUST_LATITUDE': Latitude coordinate of the customer.
            - 'CUST_LONGITUDE': Longitude coordinate of the customer.
             **Note**: This data relates to the invoices table, can merge to add the data of the invoices over the customer code.
            
    7. **DW_DIM_INDUSTRIES** ('industry_df'):
        - **Description**: The industries and their names.
            - `INDUSTRY`:  Industry name.
            - `INDUSTRY_CODE`:  The id of Diplomat's different industries  (primary key).
        **Note**: This data relates to the invoices table, can merge to add the data of the invoices over the industry code.
            

    8. **DW_DIM_MATERIAL** ('material_df')
        - **Description**: The materials and their attributes.
            - `MATERIAL_NUMBER`: The id of Diplomat's items (primary key). 
            - `MATERIAL_EN`: Item name in english. 
            - `MATERIAL_HE`: Item name in hebrew.
            - `MATERIAL_DIVISION`: Type of item (mainly food ot toiletics).
            - 'BRAND_HEB': The brand of the item in hebrew.
            - 'BRAND_ENG': The brand of the item in english.
            - 'SUB_BRAND_HEB': The sub brand of the item in hebrew.
            - 'SUB_BRAND_ENG': The sub brand of the item in english.            
            - 'CATEGORY_HEB': The category of the item in hebrew.
            - 'CATEGORY_ENG': The category of the item in english.
            - 'SUPPLIER_HEB': The supplier of the item in hebrew.
            - 'SUPPLIER_ENG': The supplier of the item in english.
            - 'BARCODE_EA': the barcode of a single item.
            - 'SALES_UNIT': the item's sales unit.
            - 'BOXING_SIZE': the item's number of single units being sold in the sales unit.
        **Note**: This data relates to the invoices table, can merge to add the data of the invoices over the material code/ number.
            
        
    this is the code that already loaded the data to the IDE:

    ```python
    db_password = os.getenv('DB_PASSWORD')
    def load_data():  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                            server='diplomat-analytics-server.database.windows.net',  
                            database='Diplochat-DB',  
                            uid='analyticsadmin', pwd=db_password)  
    

        #Define tables and queries
        tables = {
            'DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': \"\"\"
                SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
                FROM [dbo].[DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
                WHERE Day BETWEEN '2023-12-31' AND '2024-09-01'
            \"\"\",
            'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': \"\"\"
            [Query]
            \"\"\",
            'DW_CHP_AGGR': \"\"\"
            [Query]
            \"\"\"
            ,
            'AGGR_MONTHLY_DW_INVOICES':\"\"\"
            [Query]
            \"\"\"
            ,
            'DW_DIM_CUSTOMERS':\"\"\"
            [Query]
            \"\"\",
            'DW_DIM_INDUSTRIES':
            \"\"\"
            [Query]
            \"\"\",
            'DW_DIM_MATERIAL':
            \"\"\"
            [Query]
            \"\"\"
        }

        dataframes = {}  
        for table, query in tables.items():  
            chunks = []  
            chunk_size = 10000  
            total_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM ({query}) AS count_query", conn).iloc[0, 0]  
            total_chunks = (total_rows // chunk_size) + 1  
    
            for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunk_size)):  
                chunks.append(chunk)  
            
            df = pd.concat(chunks, ignore_index=True)  
            dataframes[table] = df  
    
        conn.close()  
        return dataframes  

    dataframes = load_data()

    dt_df = create_date_dataframe(start_date, end_date)
    dataframes['DATE_HOLIAY_DATA'] = dt_df

    # Assigning dataframes to variables
    stnx_sales = dataframes['DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    chp = dataframes['DW_CHP_AGGR']
    dt_df = dataframes['DATE_HOLIAY_DATA']
    inv_df = dataframes['AGGR_MONTHLY_DW_INVOICES']
    customer_df = dataframes['DW_DIM_CUSTOMERS']
    industry_df = dataframes['DW_DIM_INDUSTRIES']
    material_df = dataframes['DW_DIM_MATERIAL']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    chp['DATE'] = pd.to_datetime(chp['DATE'])
    dt_df['DATE'] = pd.to_datetime(dt_df['DATE'])
    inv_df['DATE] = pd.to_datetime(inv_df['DATE'])
    ```

    The names of the brands of diplomat in storenext (stnx_items)-
        אולוויז, אולדייז, פנטן, סאקלה, פמפרס, טמפקס, אריאל, Fairy, אוראל בי, הד&שולדרס, קוטדור, לוטוס, טייד, לנור, קראפט, מילקה, LU, סקיני טינס, קולמנס, גולדן בריק, HP, אוראו, וולה, אוריינטל פוד, דורסל, Skippy, פרינגלס, קיקומן, גילט, קולסטון נטורלס, הרבל אסנס, Walkers, ג'השאן , בראון, קולסטון קיט, אוזי, בונזו, שזיר, סטארקיסט, הלטי בוי, רומו, Lazaro, ביונד מיט, לור, נוטרילון, גיקובס, מזולה, סופר קאט, היינץ, לחם יין, קלוגס, לה קט, Lo Salt, SUPREME, ויוה קולור, נטורלה, רג'יה, קרסט, אולד ספייס, קולסטון-2000, ויולייף, דרפורד, טובלרון, מקסוול, אל ארז, דקורה, ביאסק, ריין דיז'ון,  - לא ידוע-1, וולהפלקס, יונה, פרופה, אורגניק אינדיה, נון, הרמזית, All In, קוסקא, Mission, יורוקיטי, דורות, נסיך הים, סיקרט, לה ניוקריה, סופר קט, יוניבר, פראוד, פטי, לגונה, קרם קולור, מנישביץ, מאיר את בייגל, קדבורי, גקובזי, דורו, מסטמכר, בארני, פנדה, קולסטון רוט, Arifoglu, בלובנד, מילוטל, פלנטרס, לוריאל, סופט קולור, OXO, מרום את קונפינו, 7 Days, קולסטון אינטנס, ציריו, וולה דלוקס, ויטקראפט, פורטוס, א. קנטינה, אופיסר, לאב דוג, משק ווילר, סוויטאנגו, איליי, אונלי, קאפוטו, אינאבה, סינגה
    
    The names of the categories (brand equivalent in stnx_items ) of diplomat in the sell-in data (material_df)-    
        Hermes, Gillette, Oral-B, Ketchup, Mustard, Mayonnaise, Personal Diagnostc, Batteries, Starkist, Yona, Cats Sand, La Cat, Reine De Dijon Mustard, Mestemacher, Losalt, Balsamic Vinegar F.S, Sauces, Fem Care, Hair Care, Reine De Dijon Mustard F.S, Biscuit, APDO, Bonzo, ORAL-B, Chocolate, S.Aids P&G, Coty, Red Bull, Vinegar, Biscuits, Mashes, Preserves, Alcohol, Spices, Powders, Coatings & Dry Foods, Rice, Kikkoman, Accessoris, Liquids, Noodles, Jams & Bakery products, Braun, Laundry Cleaning, Spread, Seaweeds, Snacks, BABY CARE, FEMCARE, HAIR CARE, HEALTHCARE, SHAVECARE, PERSONAL CARE, ORAL CARE, LAUNDRY, Oriental Retail, Soy Sauce, Kellogg's Coranflakes, HOT BEVERAGES, COFFEE, ASSORTMENTS, COUNTLINES, TABLETS, GROCERY, CHEWING GUM, CANDY, BUBBLE GUM, Nutrilon Premium, Sweet spreads, Balsamic Vinegar, Spices, Flour, Pasta, Delicatessen, Tomato products, Fairy, Pringlesoperational organization, Badagoni, Health, Personal Care, Oral Care, Wellaflex, Shave Care, Duracell, Kaija, Henrys Kitchen, Blu, Sukrazit, Infant Formula, Growing-Up Milk, Infant Cereals, Glavproduct, Coffee SD, Coffee FD, Coffee Mixes, Cocoa, Tablets, Count lines, Confetti, Other Retail Chocolate, Sugar, Culinary, Cereals, Darling, Pro Plan, Baby Care, Meals and Drinks, Pringles, Fabric Care, Home Care, Friskies, Ace, Jacobs, Felix, Bars, Crumbs, BISCUITS, Wellaton, Beans, Feminine care, Lor, Gurieli, Comet, Golden Brake, Tumin Portos, Bistroff, Lenor Fabric Care, Private Label, Super Cat, Food pallets, Gomi, Takeaway, PILCHARDS, Beyond Meat, Illy, Londacolor, Clean World, Tortilla New, Face Care, PHILIPS BATTERIES, Kula, Bear, Cheese Subtitute, Violife-NA, Vegan, S-A JDE, Lotus, Dugladze, HEINZ, HP, L&P, Appliances, Cream, Proud, Canned Fish, Margarine, BOXD WATER, SAUCES, LEMON JUICE, SPICES, Oil, Wella Pro, Kadus, S-A Duracell, TOMATO, Nescafe Big pack Coffee, All In, CULINARY, GRAIN, BABY, HPC, SNACKS AND TREATS, INGWE RANGE, WUPPERTHAL, NATURAL FOODS, NA, Italian Food, Allin, Cherie, Local Accessories, LOTUS BISCOFF, BEVERAGES, HOMECARE, Chipta, Blue Frog, PERSONAL HEALTH CARE, Purina One, Loacker, Dog Chow, Cat Chow, Elchim, Vaporia, Wella Deluxe, Mincos, Sweetango, Milotal, Panda, Kellogg's Children Cereals, Soft Color, S-A Meditrend, Starkist Pallets, Barebells, Preserved, S-A Mondelez, SNACKS, MasterChef, SOYA MINCE, Mondelez, Re Export, S-A Lotus, Asian Food, PAPA JOHNS, Retail, Color Perfect, Ritter Sport, Kiddylicious, Local, Sea Food, Twinings, Jarrah, Hard Cheese, Garlic, Herbs, Seasoning, Sauce, MEN DEO, WOMAN DEO, READY-TO-DRINK, Grenade, מתנות חברה, ROYCO, Blue Band - NA, Others, Spirits, PIO -S-A, Starkist F.S., Kellogg's, S-A Wella Professional, Hot Chocolate, Greek, P&G Pallets, Industrial, Horeca Equipment

    important industries (industry_df) - Supersal, Rami Levi, Beitan, Victory, 4Ch - Others 17, SuperPharm, Osher ad/Merav mazon, Yochananof, Hazi Hinam, Platinum.

    Quesstions Convention - 

    For any question you provide a code in python and in the end give the the answer in a python text variable named 'answer' after making the needed analysis.

    * A very important note on Predictive analytics! - when asked about a future event, make a forecast based on forcasting methods such as SARIMA to get the desired prediction (make sure to deactivate any printed output from the chosen model).

    Context for the Questions of stakeholders:

    >Market Cap/Share (נתח שוק) - The Percent of total sales in NIS of a certain brand in his category by default or any other field if specifically requested by the user - 
    meaning if asked about a certain brand's market cap, then you need sum that brand's sales in the chosen time frame (Can be monthly, weekly or daily, group your dataframe accordingly), and devide it by the total sales in that brand's category,
    you need to merge the stnx_sales and stnx_items dataframes to obtain all the neccesary data for that.

    >textual data - all the textual data here is hebrew so take that in mind while filtering dataframes.

    >Competitors (מתחרים) - When requesting data about competitors, we are the supplier name 'דיפלומט' in the data and other supliers in the same category/ requested user's field are the competition. 

    >Promotion Sales (מבצעים) - It is an actual promotion only where the 'AVG_SELLOUT_PRICE' a non-negative float number value. 
    Final reminder: ensure that the 'answer' variable resembles a genuine prompt produced by a language model in the language used to address you!
    """
    sys_msg += f'\nYour operation present date is {datetime.now()}.'

    sys_error = """  
    You are an assistant that informs the user when their input is unclear,  
    and you ask them to provide more details or rephrase their message in the same language they used.  
    """  

    examples = [{'role':'user','content':"Who is the smallest customer in 4 chain?"},
        {'role': 'assistant',
        'content': "```python\nindustry = '4Ch - Others 17'  \nindustry_code = industry_df[industry_df.INDUSTRY == industry].INDUSTRY_CODE.values[0]\n\ninv_df[inv_df.INDUSTRY_CODE == industry].groupby('CUSTOMER_CODE').agg({'Gross':'sum'})\n\ncust_inv_grp_df = inv_df[inv_df.INDUSTRY_CODE == industry_code].groupby('CUSTOMER_CODE').agg({'Gross':'sum'}).reset_index()\ncust_inv_grp_df = cust_inv_grp_df.merge(customer_df[['CUSTOMER_CODE','CUSTOMER']],how = 'left').sort_values('Gross')\ncust_inv_grp_df = cust_inv_grp_df[cust_inv_grp_df.Gross>0]\nsmallest_4ch_customer = cust_inv_grp_df.CUSTOMER.values[0]\nsmallest_4ch_money = cust_inv_grp_df.Gross.values[0]\n\nanswer = f'The smallest customer among 4 chain is {smallest_4ch_customer} with a total of just {smallest_4ch_money}'\n```\n"},
        {'role':'user','content':"Show me how well oreo is doing in this customer"},
        {'role': 'assistant',
        'content': '```python\n# Define the brand and category to check  \nbrand_to_check = \'Oreo\'    \ncategory_to_check = brand_to_check  # You can change this if needed  \n  \n# Filter materials for the specified brand and category  \nfiltered_materials = material_df[  \n    (material_df.CATEGORY_ENG.str.contains(category_to_check, na=False)) |  \n    (material_df.BRAND_ENG.str.contains(brand_to_check, na=False))  \n]  \n  \n# Identify the industry and its corresponding code  \nindustry = \'4Ch - Others 17\'    \nindustry_code = industry_df[industry_df.INDUSTRY == industry].INDUSTRY_CODE.values[0]  \n  \n# Group invoice data by customer code for the specified industry  \ncust_inv_grp_df = inv_df[inv_df.INDUSTRY_CODE == industry_code].groupby(\'CUSTOMER_CODE\').agg({\'Gross\': \'sum\'}).reset_index()  \n  \n# Merge with customer data and filter for positive gross sales  \ncust_inv_grp_df = cust_inv_grp_df.merge(customer_df[[\'CUSTOMER_CODE\', \'CUSTOMER\']], how=\'left\').sort_values(\'Gross\')  \ncust_inv_grp_df = cust_inv_grp_df[cust_inv_grp_df.Gross > 0]  \n  \n# Identify the smallest customer in the 4Ch category  \nsmallest_4ch_customer_code = cust_inv_grp_df.CUSTOMER_CODE.values[0]  \nsmallest_4ch_customer = cust_inv_grp_df.CUSTOMER.values[0]  \n  \n# Get unique material numbers from filtered materials  \nmaterial_numbers = filtered_materials.MATERIAL_NUMBER.unique()  \n  \n# Filter invoice data for the smallest customer and selected materials  \nbrand_inv_df = inv_df[  \n    (inv_df.CUSTOMER_CODE == smallest_4ch_customer_code) &  \n    (inv_df.MATERIAL_CODE.isin(material_numbers))  \n]  \n  \n# Calculate monthly sales, resampling the data  \nmonthly_sales = brand_inv_df.resample(\'M\', on=\'DATE\').agg({\'Gross\': \'sum\'}).reset_index()  \n  \n# Add a column to indicate increase or decrease in sales  \nmonthly_sales[\'Change\'] = monthly_sales[\'Gross\'].diff()  \nmonthly_sales[\'Trend\'] = monthly_sales[\'Change\'].apply(lambda x: \'Increase\' if x > 0 else (\'Decrease\' if x < 0 else \'No previous data\'))  \n  \n# Prepare the output message  \nsales_summary = []  \nfor index, row in monthly_sales.iterrows():  \n    month = row[\'DATE\'].strftime(\'%Y-%m\')  # Formatting the date  \n    gross_sales = row[\'Gross\']  \n    trend = row[\'Trend\']  \n    sales_summary.append(f"Month: {month}, Sales: {gross_sales:.2f}, Trend: {trend}")  \n  \nanswer = f"Here are the monthly sales for {brand_to_check} in {smallest_4ch_customer}:\n" + "\n".join(sales_summary)  \n```\n'},
        {'role':'user','content':"I saw that September is low on sales, show me competitor promotions in that period"},
        {'role': 'assistant',
        'content': '```python\nbrand_to_check = \'Oreo\'  \nbrand_to_check_stnx = \'אוראו\'  \nchain_name = \'סופר יודה\'\nmonth = 9  \n  \n# Initialize an empty list to collect results  \ncompetitor_promotions = []  \n  \ncategories = stnx_items[(stnx_items.Brand_Name == brand_to_check_stnx) & (stnx_items.Supplier_Name == \'דיפלומט\')].Category_Name.unique().tolist()  \n  \nfor category in categories:  \n    barcode_lst = stnx_items[(stnx_items.Category_Name == category) & (stnx_items.Supplier_Name != \'דיפלומט\')].Barcode.unique().tolist()  \n    chp_competitor = chp[(chp.BARCODE.isin(barcode_lst)) & (chp.DATE.dt.month == month) & (chp.SELLOUT_DESCRIPTION != \'\')& (chp.CHAIN.str.contains(chain_name))]  \n  \n    if len(chp_competitor) > 5:  \n        # Merge to include the Item_Name and Supplier_Name  \n        chp_competitor = chp_competitor.merge(stnx_items[[\'Barcode\', \'Item_Name\', \'Supplier_Name\']], left_on=\'BARCODE\', right_on=\'Barcode\', how=\'left\')  \n          \n        # Collect the relevant information  \n        sampled_data = chp_competitor[[\'Barcode\', \'Item_Name\', \'Supplier_Name\', \'SELLOUT_DESCRIPTION\']].sample(5).reset_index(drop = True)  \n          \n        # Append to the list  \n        for index, row in sampled_data.iterrows():\n            promo_txt = f"Barcode: {row[\'Barcode\']}, Item: {row[\'Item_Name\']}, Supplier: {row[\'Supplier_Name\']}, Description: {row[\'SELLOUT_DESCRIPTION\']}"\n            if index==0:\n                promo_txt = f"In {category}:\n{promo_txt}"\n            else:\n                pass\n            competitor_promotions.append(promo_txt)  \n          \n# Create the answer variable  \nanswer = f"{brand_to_check}\'s competitors promotions this period in {chain_name}:\n" + "\n".join(competitor_promotions)  \n```\n'},
        ]

      
    openai_api_key = os.getenv('OPENAI_KEY')

    client = AzureOpenAI(  
        azure_endpoint="https://ai-usa.openai.azure.com/",  
        api_key=openai_api_key,  
        api_version="2024-02-15-preview"  
    )

    MODEL = "Diplochat"  


    def model_reponse(prompt, sys_msg, examples=[]):  
        response = client.chat.completions.create(  
            model=MODEL,  # model = "deployment_name"  
            messages=[{"role": "system", "content": sys_msg}] + examples + [{"role": "user", "content": prompt}],  
            temperature=0.7,  
            max_tokens=2000,  
            top_p=0.95,  
            frequency_penalty=0,  
            presence_penalty=0,  
            stop=None  
        )

        return response  


    def comment_out_lines(code,print_drop = True):  
        
        
        # Define the patterns and replacements  
        lines_to_comment = [  
            "stnx_sales, stnx_items, chp = load_data()"
        ]  
            
        if print_drop:
        # Replace any print() statements with #print()  
            code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)  
        
        # Define the pattern to find everything after any of the specified lines  
        pattern = re.compile(r'=(?: *load_data\(\) *|load_data\(\))(.*)', re.DOTALL)  
        # Search the text  
        match = pattern.search(code)

        # If a match is found, extract the part after the line  
        if match:  
            code = match.group(1)  
            if code.strip().startswith('()'):
                code = code[2:]
            code = code.strip()  
        else:  
            code = code     
        return code 

    def insert_log_data(conn, log_session):  
        # Create the table if it does not exist  
        create_table_query = """  
        IF NOT EXISTS (  
            SELECT *   
            FROM INFORMATION_SCHEMA.TABLES   
            WHERE TABLE_NAME = 'AI_LOG'  
        )  
        BEGIN  
            CREATE TABLE AI_LOG (  
                ID INT IDENTITY(1,1) PRIMARY KEY,
                Conversation_ID NVARCHAR(100), 
                Timestamp DATETIME,  
                User_Name NVARCHAR(100),  
                User_Prompt NVARCHAR(MAX),  
                LLM_Responses NVARCHAR(MAX),  
                Code_Extractions NVARCHAR(MAX),  
                Final_Answer NVARCHAR(MAX),  
                Num_Attempts INT,  
                Num_LLM_Calls INT,  
                Errors NVARCHAR(MAX),  
                Total_Time FLOAT,
                User_Ratings NVARCHAR(MAX),
                Usage NVARCHAR(MAX)
            )  
        END  
        """  
        
        cursor = conn.cursor()  
        cursor.execute(create_table_query)  
        
        # Insert log data into the AI_LOG table  
        insert_query = """  
        INSERT INTO AI_LOG (Conversation_ID, Timestamp, User_Name, User_Prompt, LLM_Responses, Code_Extractions, Final_Answer, Num_Attempts, Num_LLM_Calls, Errors, Total_Time, User_Ratings, Usage)  
        VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)  
        """  
        
        cursor.execute(insert_query, log_session)  
        conn.commit()  
        cursor.close()

    def alter_log_data(conn, prompt_timestamp, user_feedback):  
        try:  
            cursor = conn.cursor()  
    
            # Define the SQL update command using parameterized query  
            sql_update_query = """  
            UPDATE [dbo].[AI_LOG]  
            SET [User_Ratings] = ?  
            WHERE [Timestamp] = ?;  
            """  
    
            # Execute the update command with parameters  
            cursor.execute(sql_update_query, (user_feedback, prompt_timestamp))  
    
            # Commit the transaction  
            conn.commit()  
    
            # print("User rating updated successfully.")  
    
        except Exception as e:  
            print("Error occurred:", e)  
    
        finally:  
            # Close the cursor  
            cursor.close()     

    def gregorian_to_hebrew(year, month, day):  
        return dates.GregorianDate(year, month, day).to_heb().hebrew_date_string()  
    
    def get_jewish_holidays(year, month, day):  
        return dates.GregorianDate(year, month, day).festival(hebrew=True)  
    
    def create_date_dataframe(start_date, end_date):  
        # Create a DataFrame with the date range  
        dt_df = pd.DataFrame({'DATE': pd.date_range(start=start_date, end=end_date)})  
        
        # Extract Hebrew dates and Jewish holidays  
        dt_df['HEBREW_DATE'] = dt_df['DATE'].apply(lambda x: gregorian_to_hebrew(x.year, x.month, x.day))  
        dt_df['HOLIDAY'] = dt_df['DATE'].apply(lambda x: get_jewish_holidays(x.year, x.month, x.day))

        
        return dt_df  

    @st.cache_data(show_spinner="Loading data.. this can take a few minutes, feel free to grab a coffee ☕") 
    def load_data(resolution_type,chp_or_invoices,sales_org):  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                            server='diplomat-analytics-server.database.windows.net',  
                            database='Diplochat-DB',  
                            uid='analyticsadmin', pwd=db_password)  
        res_tp = resolution_type
        coi = chp_or_invoices
        so = so_dict[sales_org]
        #Define tables and queries
        tables = { # AGGR_MONTHLY_DW_FACT_STORNEXT
            f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES': f"""
                SELECT Day, Barcode, Format_Name, Sales_NIS, Sales_Units, Price_Per_Unit
                FROM [dbo].[AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES]
            """,
            'DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS': """
                SELECT Barcode, Item_Name, Category_Name, Sub_Category_Name, Brand_Name, Sub_Brand_Name, Supplier_Name
                FROM [dbo].[DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS]
            """,
            f'AGGR_{res_tp.upper()}_DW_CHP': f"""
                SELECT DATE,BARCODE,CHAIN,AVG_PRICE,AVG_SELLOUT_PRICE,SELLOUT_DESCRIPTION,NUMBER_OF_STORES
                FROM [dbo].[AGGR_{res_tp.upper()}_DW_CHP]
                --WHERE [DATE] BETWEEN DATEADD(DAY, -90, GETDATE()) AND GETDATE()
            """,
            'DW_DIM_CUSTOMERS':
            """
            SELECT [CUSTOMER_CODE],
            [CUSTOMER],
            [CITY],
            [CUSTOMER_ADDRESS],
            [CUST_LATITUDE],
            [CUST_LONGITUDE]
            FROM [dbo].[DW_DIM_CUSTOMERS]
            WHERE CUSTOMER_CODE IN (SELECT DISTINCT CUSTOMER_CODE FROM [dbo].[AGGR_MONTHLY_DW_INVOICES])
            """
            ,
            'DW_DIM_INDUSTRIES':
            """
            SELECT [INDUSTRY]
                ,[INDUSTRY_CODE]
            FROM [dbo].[DW_DIM_INDUSTRIES]
            """
            ,
            'DW_DIM_MATERIAL':
            f"""
            SELECT [MATERIAL_NUMBER]
                ,[MATERIAL_EN]
                ,[MATERIAL_HE]
                ,[MATERIAL_DIVISION]
                ,[BRAND_HEB]
                ,[BRAND_ENG]
                ,[SUB_BRAND_HEB]
                ,[SUB_BRAND_ENG]
                ,[CATEGORY_HEB]
                ,[CATEGORY_ENG]
                ,[BARCODE_EA]
	            ,[SALES_UNIT]
	            ,[BOXING_SIZE]
            FROM [dbo].[DW_DIM_MATERIAL] 
            WHERE MATERIAL_NUMBER IN (SELECT DISTINCT MATERIAL_CODE FROM [dbo].[AGGR_MONTHLY_DW_INVOICES])
            """
            ,
            'AGGR_MONTHLY_DW_INVOICES':
            f"""
            SELECT [DATE]
                ,[SALES_ORGANIZATION_CODE]
                ,[MATERIAL_CODE]
                ,[INDUSTRY_CODE]
                ,[CUSTOMER_CODE]
                ,[Gross]
                ,[Net]
                ,[Net VAT]
                ,[Gross VAT]
                ,[Units]
            FROM [dbo].[AGGR_MONTHLY_DW_INVOICES]
            WHERE [SALES_ORGANIZATION_CODE] = '{so}'
            """,
            'AI_LOG':"""
            SELECT [ID]
                ,[Conversation_ID]
                ,[Timestamp]
                ,[User_Name]
                ,[User_Prompt]
                ,[LLM_Responses]
                ,[Code_Extractions]
                ,[Final_Answer]
                ,[Num_Attempts]
                ,[Num_LLM_Calls]
                ,[Errors]
                ,[Total_Time]
                ,[User_Ratings]
                ,[Usage]
            FROM [dbo].[AI_LOG]
            """
        }

        # Filter the tables based on the coi variable  
        filtered_tables = {  
            key: value for key, value in tables.items()   
            if key != 'AGGR_MONTHLY_DW_INVOICES' and coi == 'CHP' or  
            key != f'AGGR_{res_tp.upper()}_DW_CHP' and coi == 'Invoices'  
        }  


        dataframes = {}  
        for table, query in filtered_tables.items():  
            chunks = []  
            chunk_size = 10000  
            total_rows = pd.read_sql_query(f"SELECT COUNT(*) FROM ({query}) AS count_query", conn).iloc[0, 0]  
            total_chunks = (total_rows // chunk_size) + 1  
    
            for i, chunk in enumerate(pd.read_sql_query(query, conn, chunksize=chunk_size)):  
                chunks.append(chunk)  
            
            df = pd.concat(chunks, ignore_index=True)  
            dataframes[table] = df  
        conn.close()

        start_date = dataframes[f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].min()
        end_date = dataframes[f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']['Day'].max()
        
        dt_df = create_date_dataframe(start_date, end_date)
        dataframes['DATE_HOLIAY_DATA'] = dt_df
        return dataframes  

    # Function to check if the text contains Hebrew characters
    def is_hebrew(text):
        return any('\u0590' <= char <= '\u05FF' for char in text)

    # similariry for dynamic examples
    # Initialize tokenizer  
    enc = tiktoken.encoding_for_model("gpt-4o-mini")  

    def binirizer_vectors(a, b):  
        # Create a combined set of unique elements from both lists  
        ab = np.array(list(set(a + b)))  
        
        # Create binary vectors for each input list  
        a_binary = np.array([1 if i in a else 0 for i in ab])  
        b_binary = np.array([1 if i in b else 0 for i in ab])  
        
        return a_binary, b_binary  
    
    def cosine_similarity(a, b):  
        return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))  
    
    def cosine_binarizer(a, b, encoder):
        # encode using encoder
        a, b = encoder.encode(a),encoder.encode(b)

        # Get binary vectors for the input lists  
        a_binary, b_binary = binirizer_vectors(a, b)  
        
        # Calculate and return the cosine similarity  
        return cosine_similarity(a_binary, b_binary)  

    def get_top_similar_prompts(log_df, user_prompt, top_n=3, current_user='Yonatan Rabinovich', date_from = datetime(2024,9,16)):  
        # Filter for liked examples
        text_or_regex  = '👍'
        # text_or_regex  = r'(?=.*👍)(?=.*מצוין עבור הנהלת הקבוצה)'  
        
        liked_example_cond = log_df.User_Ratings.str.contains(text_or_regex, regex=True)  
    
        # If a current user is provided, filter by user name  
        user_cond = log_df.User_Name == current_user if current_user else True  
        
        date_from_cond = log_df.Timestamp>=date_from

        # Combine conditions  
        conds = liked_example_cond & user_cond  & date_from_cond
        cond_log_df = log_df[conds].copy().reset_index(drop=True)  
    
        # Calculate similarities  
        similarities = [cosine_binarizer(row['User_Prompt'], user_prompt, enc) for index, row in cond_log_df.iterrows()]  
    
        # Get top `top_n` similar prompts  
        top_indices = np.argsort(similarities)[-top_n:]#[::-1]  
        dynammic_examples = cond_log_df.loc[top_indices].copy()  
    
        # Prepare the list of dynamic examples  
        dynammic_examples_lst = []  
        
        for _, x in dynammic_examples.iterrows():  
            user_role = {'role': 'user', 'content': x['User_Prompt']}  
            assistant_role = {'role': 'assistant', 'content': eval(x['Code_Extractions'])[0]}  
            dynammic_examples_lst.append(user_role)  
            dynammic_examples_lst.append(assistant_role)  
    
        return dynammic_examples_lst
    
    def extract_code(txt):  
        pattern = r'python(.*?)'  
        all_code = re.findall(pattern, txt, re.DOTALL)  
        if len(all_code) == 1:  
            final_code = all_code[0]  
        else:  
            final_code = '\n'.join(all_code)  
        return final_code
    
    
    user_name = st.session_state.get("name", "Guest")  # Default to "Guest" if not set
    
    # ensure weekly is default
    res_tp = st.session_state.get('resolution_type','weekly')
    
    # ensure chp is default
    coi = st.session_state.get('chp_or_invoices','invoices')

    so = st.session_state.get('sales_org','1000')
    
    st.title(f"{user_name} {res_tp.capitalize()} Sales Copilot 🤖")  
    

    dataframes = load_data(res_tp,coi,so)  
    
    # Assigning dataframes to variables
    stnx_sales = dataframes[f'AGGR_{res_tp.upper()}_DW_FACT_STORENEXT_BY_INDUSTRIES_SALES']
    stnx_items = dataframes['DW_DIM_STORENEXT_BY_INDUSTRIES_ITEMS']
    customer_df = dataframes['DW_DIM_CUSTOMERS']
    industry_df = dataframes['DW_DIM_INDUSTRIES']
    material_df = dataframes['DW_DIM_MATERIAL']
    dt_df = dataframes['DATE_HOLIAY_DATA']
    log_df = dataframes['AI_LOG']

    # Convert date columns to datetime
    stnx_sales['Day'] = pd.to_datetime(stnx_sales['Day'])
    dt_df['DATE'] = pd.to_datetime(dt_df['DATE'])

    # duplication drop
    customer_df = customer_df.drop_duplicates(subset = ['CUSTOMER_CODE'])
    material_df = material_df.drop_duplicates(subset = ['MATERIAL_NUMBER'])


    #optional data
    if coi=='CHP':
        chp = dataframes[f'AGGR_{res_tp.upper()}_DW_CHP']
        chp['DATE'] = pd.to_datetime(chp['DATE'])
    else:
        inv_df = dataframes['AGGR_MONTHLY_DW_INVOICES']
        inv_df['DATE'] = pd.to_datetime(inv_df['DATE'])    


    user_avatar = '🧑'

    client = AzureOpenAI(  
        azure_endpoint="https://ai-usa.openai.azure.com/",  
        api_key=openai_api_key,  
        api_version="2024-02-15-preview"  
    )  
    MODEL = "Diplochat"  
    
    # Initialize log_dfs 
    if 'log_dfs' not in st.session_state:  
        st.session_state.log_dfs = []  
    
    if "openai_model" not in st.session_state:  
        st.session_state["openai_model"] = MODEL 
    
    if 'user_feedback' not in st.session_state:  
        st.session_state.user_feedback = ''

    if 'user_feedback_lst' not in st.session_state:
        st.session_state.user_feedback_lst = []

    if "messages" not in st.session_state:  
        st.session_state.messages = [{"role": "system", "content": sys_msg}]


    if "base_history" not in st.session_state:  
        st.session_state.base_history = [{"role": "system", "content": sys_msg}]+examples




    def handle_feedback():
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                        server='diplomat-analytics-server.database.windows.net',  
                        database='Diplochat-DB',  
                        uid='analyticsadmin', pwd=db_password)    
        
        st.session_state.user_feedback_lst.append(st.session_state.user_feedback)
        st.session_state.log_dfs = st.session_state.log_dfs[:-1]
        # add to log frame
        tmp_df.loc[0,'User_Ratings'] = str(st.session_state.user_feedback)
        st.session_state.log_dfs.append(tmp_df)
        # add to sql
        # Call the function  
        alter_log_data(conn, tmp_df.loc[0,'Timestamp'], str(st.session_state.user_feedback))  
    
        # Close the connection  
        conn.close() 
        st.toast("✔️ Feedback received!")
        
    answer = ''


    # Display chat messages from history on app rerun  
    for message in st.session_state.messages:  
        if message["role"] == 'assistant':  
            with st.chat_message(message["role"], avatar='🤖'):                    
                st.markdown(message["content"], unsafe_allow_html=True)

        elif message["role"] == 'user':  
            with st.chat_message(message["role"], avatar=user_avatar):               
                st.markdown(message["content"])


    log_data = []
    # data in each session: prompt,txt_content,code_lst,
    log_session = []

    log_cols = ['Conversation_ID','Timestamp','User_Name','User_Prompt','LLM_Responses','Code_Extractions','Final_Answer','Num_Attempts','Num_LLM_Calls','Errors','Total_Time','User_Ratings','Usage']
    log_dfs = []

    israel_tz = pytz.timezone("Asia/Jerusalem")
    
    now = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
    name_id = ''.join(i for i in user_name if i==i.upper()).replace(' ','')
    ts_id = ''.join(i for i in now if i.isdigit())

    conv_id = f'{name_id}_{ts_id}'
    # Initialize the conversation ID in session state if it doesn't exist  
    if 'conv_id' not in st.session_state:  
        st.session_state.conv_id = f'{name_id}_{ts_id}'


    m_limit = 25
    if 'memory_limit' not in st.session_state:
        st.session_state.memory_limit = m_limit

    n_most_similar = 3
    if 'n_most_similar' not in st.session_state:
        st.session_state.n_most_similar = n_most_similar
        
    if prompt := st.chat_input("Ask me anything"):
        prompt_timestamp = datetime.now(israel_tz).strftime("%Y-%m-%d %H:%M:%S") 
        st.session_state.messages.append({"role": "user", "content": prompt})
        
        dynamic_examples_lst = get_top_similar_prompts(log_df, prompt, top_n=st.session_state.n_most_similar)

        # base history
        st.session_state.base_history[1:1+st.session_state.n_most_similar*2] = dynamic_examples_lst
        
        # training base history - premade examples
        # st.session_state.base_history[1:1+st.session_state.n_most_similar*2] = examples

        st.session_state.base_history.append({"role": "user", "content": prompt})
        
        with st.chat_message("user", avatar=user_avatar):
            st.markdown(prompt)

            start_time = time.time()
        

        with st.spinner("Thinking..."):
            
            answer = ''
            txt = ''
            
            max_attempts = 10
            errors = []
            attempts = 1
            txt_content_lst = []
            code_lst = []
            n_llm_api_call = 0

            usage_dict = {}
            py_attempts_usage_lst = []

            while attempts < max_attempts:
                
                try:

                    txt = client.chat.completions.create(
                        model=st.session_state["openai_model"],
                        messages=[
                            {"role": m["role"], "content": m["content"]}
                            for m in st.session_state.base_history
                        ],
                        max_tokens=2000,
                        stream=False,
                    )
                    txt_content = txt.choices[0].message.content

                    py_usage_dict = txt.to_dict()['usage']
                    py_attempts_usage_lst.append(py_usage_dict)


                    
                    n_llm_api_call+=1
                    # append original gen ai content to the list
                    txt_content_lst.append(txt_content)
                
                    # Regex pattern to extract the Python code
                    pattern = r'```python(.*?)```'   
                    all_code = re.findall(pattern, txt_content, re.DOTALL)
                    if len(all_code) == 1:  
                        code = all_code[0]
                        
                    else:  
                        code = '\n'.join(all_code)              
                    
                
                    # Use re.sub to comment out any print statement  
                    code = re.sub(r"^(\s*)print\(", r"\1#print(", code, flags=re.MULTILINE)
                    # Use re.sub to comment out any import statement  
                    code = re.sub(r"^(\s*)import\s", r"\1#import ", code, flags=re.MULTILINE)  
                    
                    if coi=='CHP':
                        local_context = {'chp':chp,'inv_df':inv_df,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'pd':pd,'np':np,'dt_df':dt_df,'customer_df':customer_df,'industry_df':industry_df,'material_df':material_df,'base64':base64,'BytesIO':BytesIO,'plt':plt}
                    else:
                        local_context = {'chp':chp,'inv_df':inv_df,'stnx_sales':stnx_sales,'stnx_items':stnx_items,'customer_df':customer_df,'industry_df':industry_df,'material_df':material_df,'dt_df':dt_df,'pd':pd,'np':np,'base64':base64,'BytesIO':BytesIO,'plt':plt}

                    exec(code, {}, local_context)
                    answer = local_context.get('answer', "No answer found.") 
                    
                    if answer == "No answer found.":  
                        raise ValueError("No answer found.")  
                    
                    sys_decorator = f"""
                    You are an AI assistant designed to enhance the quality and presentation of responses from the perspective of Diplomat Distributors Ltd. 
                    Your task is to refine generated content, making it more articulate and visually appealing, 
                    while preserving all original numbers and facts. Ensure that the output reflects a professional tone suitable
                    for internal discussions and communications within the company.
                    notes for the data you may be given:
                    > percentage - round to the second digit after the dot and add the percentage symbol - 2.22222 --> 2.22%.
                    > money - make the ILS currency symbol, round to the second digit after the dot - 1332.22222 --> 1,332.22₪.
                    > quantity - always show as an integer and round to 0 digits after the dot - 2.22222 --> 2.
                    > dates - format like this: dd/mm/yyyy - 2024-01-31 --> 31/01/2024.



                    finally: ensure that the your response is in the language used by your recieved input, and is presenting information and insights to the user.
                    """

                    # Another key point: If the user includes a greeting or farewell message, address them by their first name. For instance, if my username is Jake Paul, respond with [greeting in the user's language] and Jake.
                    # This user's name is {user_name}

                    answer_has_plot = 'data:image/png;base64' in answer

                    if not answer_has_plot:
                        decorator_response = model_reponse(answer, sys_decorator)
                        answer = decorator_response.choices[0].message.content.strip()
                        
                        decorator_usage_dict = decorator_response.to_dict()['usage']
                        error_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}
                        
                        n_llm_api_call+=1

                    else:
                        decorator_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}
                        error_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}

                    history_msg = f"```python{code}```"

                    with st.chat_message("assistant", avatar='🤖'):
                    
                        if not answer_has_plot:
                            # Create a placeholder for streaming output 
                            placeholder = st.empty()  
                            streamed_text = ""  

                            # Stream the answer output  
                            for char in answer:  
                                streamed_text += char  
                                placeholder.markdown(streamed_text)  
                                time.sleep(0.01)  # Adjust the sleep time to control the streaming speed 
                        else:
                            # Create a placeholder for streaming output 
                            placeholder = st.empty()  
                            streamed_text = ""  

                            # Stream the answer output  
                            for char in answer:
                                streamed_text += char
                                placeholder.markdown(streamed_text)

                                # Check if streamed_text endswith ('<img src=')
                                if streamed_text.endswith('<img src='):
                                    # Display the remainder of the answer starting from the current position
                                    streamed_text+=answer[len(streamed_text):]
                                    placeholder.markdown(streamed_text,unsafe_allow_html=True)
                                    break
                                time.sleep(0.01)  # Adjust the sleep time to control the streaming speed 

                    st.session_state.base_history.append({"role": "assistant", "content": history_msg})
                    
                    # append regex formatted code with python markdown content to the list
                    code_lst.append(history_msg)
                    break

                except Exception as e:  
                    errors.append(f"Attempt {attempts} failed: {e}")  
                    attempts += 1
                    
            # generate anwer for failures
            if attempts == max_attempts:
                # replace with ai generated text
                # prompt = f"""The user, {user_name}, has asked: "{prompt}", please clarify that their request is not clear to you and make sure to include their first name in your response. 
                # unless it is a hello or goodbye greeting in which case respond normally with their first name.
                # For instance, if my username is Jake Paul, respond with [greeting in the user's language] and Jake."""

                error_response = model_reponse(prompt, sys_error)    
                answer = error_response.choices[0].message.content.strip()

                decorator_usage_dict = {'completion_tokens': 0, 'prompt_tokens': 0, 'total_tokens': 0}
                error_usage_dict = error_response.to_dict()['usage']
                 

                n_llm_api_call+=1
                with st.chat_message("assistant", avatar='🤖'):
                    # Create a placeholder for streaming output  
                    placeholder = st.empty()  
                    streamed_text = ""  
                    
                    # Stream the answer output  
                    for char in answer:  
                        streamed_text += char  
                        placeholder.markdown(streamed_text)  
                        time.sleep(0.01)  # Adjust the sleep time to control the streaming speed
                
                # delete the last question from base_history
                st.session_state.base_history = st.session_state.base_history[:-1]

            st.session_state.messages.append({"role": "assistant", "content": answer})
            
            usage_dict['py_attempts_usage'] = py_attempts_usage_lst
            usage_dict['decorator_usage'] = decorator_usage_dict
            usage_dict['error_usage'] = error_usage_dict
            

            # memory control
            if len(st.session_state.base_history)>st.session_state.memory_limit:
                # delete the first 2 massages after system and dinamic samples
                # st.session_state.base_history = [st.session_state.base_history[0]]+st.session_state.base_history[3:]
                st.session_state.base_history = st.session_state.base_history[0:(1+st.session_state.n_most_similar*2)]+st.session_state.base_history[1+st.session_state.n_most_similar*2+2:]

            elapsed_time = time.time() - start_time

            # append rest of the data from the session
            empty_rating = ''

            log_session.append(st.session_state.conv_id)
            log_session.append(prompt_timestamp)
            log_session.append(user_name)
            log_session.append(prompt)
            log_session.append(str(txt_content_lst))
            log_session.append(str(code_lst))
            log_session.append(answer)
            log_session.append(attempts)
            log_session.append(n_llm_api_call)
            log_session.append(str(errors))
            log_session.append(elapsed_time)
            log_session.append(empty_rating)
            log_session.append(str(usage_dict))
            
            
        log_data.append(log_session)
        
        # Insert log data into SQL table  
        conn = pyodbc.connect(driver='{ODBC Driver 17 for SQL Server}',  
                              server='diplomat-analytics-server.database.windows.net',  
                              database='Diplochat-DB',  
                              uid='analyticsadmin', pwd=db_password)  
        insert_log_data(conn, log_session)  
        conn.close()  
        
        tmp_df = pd.DataFrame(log_data,columns=log_cols)
        # log_dfs.append(tmp_df)

        st.session_state.log_dfs.append(tmp_df)
        
        
        # log_df = pd.concat(log_dfs,axis=0).reset_index(drop = True)
        log_df = pd.concat(st.session_state.log_dfs, axis=0).reset_index(drop=True)
        

        if user_name in admin_list:
            # Create an expander  
            with st.expander("Show Log DataFrame"):  
                # Your code inside the expander  
                st.dataframe(log_df)
                st.markdown(st.session_state.base_history)# for debug
                # st.markdown(usage_dict)
            
        # Feedback mechanism


        with st.form('form'):
            streamlit_feedback(feedback_type="thumbs",
                                optional_text_label="Enter your feedback here", 
                                align="flex-start", 
                                key='user_feedback')
            st.form_submit_button('Save feedback', on_click=handle_feedback)    

elif st.session_state['authentication_status'] is False:  
    st.error('Username/password is incorrect')  
elif st.session_state['authentication_status'] is None:  
    st.warning('Please enter your username and password')  

    
    