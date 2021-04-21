class SqlQueries:
    create_staging_listings = ("""
        DROP TABLE IF EXISTS STAGING_LISTINGS;
        CREATE TABLE public.STAGING_LISTINGS (
                                    LISTING_ID INT NOT NULL, 
                                    HOST_ID INT, 
                                    HOST_URL NVARCHAR(MAX) NULL,
                                    HOST_NAME NVARCHAR(MAX) NULL,                                     
                                    HOST_SINCE DATE, 
                                    HOST_LOCATION NVARCHAR(MAX), 
                                    HOST_IS_SUPERHOST NVARCHAR(MAX), 
                                    HOST_RESPONSE_TIME NVARCHAR(MAX), 
                                    HOST_RESPONSE_RATE NVARCHAR(MAX), 
                                    HOST_ACCEPTANCE_RATE NVARCHAR(MAX), 
                                    HOST_VERIFICATIONS NVARCHAR(MAX), 
                                    HOST_IDENTITY_VERIFIED NVARCHAR(MAX), 
                                    HOST_HAS_PROFILE_PIC NVARCHAR(MAX),                                     
                                    HOST_TOTAL_LISTINGS_COUNT FLOAT,
                                    NEIGHBOURHOOD_PIN INT,
                                    PROPERTY_TYPE NVARCHAR(MAX),
                                    ROOM_TYPE NVARCHAR(MAX),
                                    ACCOMMODATES INT,
                                    BATHROOMS_TEXT NVARCHAR(MAX) NULL, 
                                    BEDROOMS FLOAT, 
                                    BEDS FLOAT,                                     
                                    AMENITIES NVARCHAR(MAX) NULL,                                     
                                    MINIMUM_NIGHTS INT,
                                    MAXIMUM_NIGHTS INT,
                                    NUMBER_OF_REVIEWS INT,                                     
                                    REVIEW_SCORES_RATING FLOAT, 
                                    REVIEWS_PER_MONTH FLOAT,
                                    PROPERTY_AREA NVARCHAR(MAX));
                                    """)

    create_staging_calendar = ("""
        DROP TABLE IF EXISTS STAGING_CALENDAR;
        CREATE TABLE public.STAGING_CALENDAR (
            LISTING_ID DECIMAL(20,0) NOT NULL,
            CALENDAR_DATE DATE, 
            AVAILABLE VARCHAR, 
            PRICE DECIMAL, 
            ADJUSTED_PRICE DECIMAL, 
            MINIMUM_NIGHTS DECIMAL(20,0), 
            MAXIMUM_NIGHTS DECIMAL(20,0));
     """)

    create_staging_reviews = ("""
        DROP TABLE IF EXISTS STAGING_REVIEWS;
        CREATE TABLE public.STAGING_REVIEWS (
            LISTING_ID INT NOT NULL, 
            REVIEW_ID DECIMAL(20,0) NULL, 
            REVIEW_DATE DATE NULL,
            REVIEWER_ID DECIMAL(20,0) NULL, 
            REVIEWER_NAME NVARCHAR(MAX) NULL, 
            COMMENTS NVARCHAR(MAX) NULL,
            AREA NVARCHAR(MAX) NULL);
    """)

    create_dim_hosts = ("""
        DROP TABLE IF EXISTS DIM_HOSTS;
        CREATE TABLE IF NOT EXISTS public.DIM_HOSTS (
            HOST_ID INT NOT NULL, 
            HOST_NAME NVARCHAR(MAX), 
            HOST_URL NVARCHAR(MAX), 
            HOST_SINCE DATE, 
            HOST_LOCATION NVARCHAR(MAX), 
            HOST_IDENTITY_VERIFIED NVARCHAR(MAX), 
            HOST_IS_SUPERHOST NVARCHAR(MAX), 
            HOST_RESPONSE_TIME NVARCHAR(MAX), 
            HOST_RESPONSE_RATE NVARCHAR(MAX), 
            HOST_ACCEPTANCE_RATE NVARCHAR(MAX), 
            HOST_VERIFICATIONS NVARCHAR(MAX), 
            HOST_HAS_PROFILE_PIC NVARCHAR(MAX), 
            HOST_TOTAL_LISTINGS_COUNT FLOAT, 
            LISTING_ID INT, 
            CONSTRAINT DIM_HOSTS_PKEY PRIMARY KEY (HOST_ID));
    """)

    create_dim_reviews = ("""
        DROP TABLE IF EXISTS DIM_REVIEWS;
        CREATE TABLE IF NOT EXISTS public.DIM_REVIEWS (
            REVIEW_ID DECIMAL(20,0) NOT NULL, 
            LISTING_ID DECIMAL(20,0), 
            REVIEWER_ID DECIMAL(20,0), 
            REVIEW_DATE DATE, 
            COMMENTS NVARCHAR(MAX), 
            REVIEW_SCORES_RATING DECIMAL, 
            CONSTRAINT DIM_REVIEWS_PKEY PRIMARY KEY (REVIEW_ID));
    """)

    create_dim_calendars = ("""
        DROP TABLE IF EXISTS DIM_CALENDARS;
        CREATE TABLE IF NOT EXISTS public.DIM_CALENDARS (
            CALENDAR_ID INT IDENTITY(1,1) NOT NULL, 
            LISTING_ID DECIMAL(20,0), 
            CALENDAR_DATE DATE,
            ADJUSTED_PRICE DECIMAL(20,0), 
            MINIMUM_NIGHTS DECIMAL(20,0), 
            MAXIMUM_NIGHTS DECIMAL(20,0), 
            CONSTRAINT DIM_CALENDARS_PKEY PRIMARY KEY(CALENDAR_ID)); 
    """)

    create_dim_properties = ("""
        DROP TABLE IF EXISTS DIM_PROPERTIES;
        CREATE TABLE IF NOT EXISTS public.DIM_PROPERTIES (
            LISTING_ID INT NOT NULL, 
            HOST_ID INT, 
            PROPERTY_TYPE NVARCHAR(MAX), 
            ROOM_TYPE NVARCHAR(MAX), 
            ACCOMMODATES INT, 
            BEDROOMS FLOAT, 
            BEDS FLOAT, 
            BATHROOMS_TEXT NVARCHAR(MAX), 
            AMENITIES NVARCHAR(MAX),
            NEIGHBOURHOOD_PIN INT, 
            PROPERTY_AREA NVARCHAR(MAX), 
            CONSTRAINT DIM_PROPERTIES_PKEY PRIMARY KEY (LISTING_ID));
       """)


    hosts_table_insert = ("""
        INSERT INTO DIM_HOSTS
        SELECT DISTINCT HOST_ID, HOST_NAME, HOST_URL, HOST_SINCE, HOST_IDENTITY_VERIFIED, 
            HOST_IS_SUPERHOST, HOST_RESPONSE_TIME, HOST_RESPONSE_RATE, HOST_ACCEPTANCE_RATE, 
            HOST_VERIFICATIONS, HOST_HAS_PROFILE_PIC, HOST_TOTAL_LISTINGS_COUNT
        FROM STAGING_LISTINGS
        where HOST_NAME <> 'Not Provided'
        AND HOST_SINCE <> '1990-01-01'
   """)

    reviews_table_insert = ("""
        INSERT INTO DIM_REVIEWS
        SELECT DISTINCT r.REVIEW_ID, r.LISTING_ID, r.REVIEWER_ID, r.REVIEW_DATE, 
            r.COMMENTS, l.REVIEW_SCORES_RATING 
        FROM STAGING_REVIEWS r 
        INNER JOIN STAGING_LISTINGS l on r.listing_id = l.listing_id 
        where r.comments is not null
    """)

    calendars_table_insert = ("""
        INSERT INTO DIM_CALENDARS (CALENDAR_DATE, LISTING_ID, ADJUSTED_PRICE, MINIMUM_NIGHTS, MAXIMUM_NIGHTS)
        SELECT DISTINCT c.CALENDAR_DATE, c.LISTING_ID, c.ADJUSTED_PRICE, c.MINIMUM_NIGHTS, c.MAXIMUM_NIGHTS 
        FROM STAGING_CALENDAR c
    """)

    properties_table_insert = ("""
        INSERT INTO DIM_PROPERTIES 
        SELECT DISTINCT l.LISTING_ID, l.HOST_ID, l.PROPERTY_TYPE, 
            l.ROOM_TYPE, l.ACCOMMODATES, l.BEDROOMS, l.BEDS, l.BATHROOMS_TEXT, l.amenities,
            l.NEIGHBOURHOOD_PIN, l.property_area 
        from STAGING_LISTINGS l
    """)
    
    create_fact_airbnb = ("""
        DROP TABLE IF EXISTS FACT_AIRBNB_AUSTIN_LA;
        CREATE TABLE IF NOT EXISTS public.FACT_AIRBNB_AUSTIN_LA (
            FACT_ID INT IDENTITY(1,1) NOT NULL, 
            HOST_ID DECIMAL(20,0), 
            LISTING_ID DECIMAL(20,0), 
            NEIGHBOURHOOD_PIN INT,
            ADJUSTED_PRICE DECIMAL(20,0), 
            NUMBER_OF_REVIEWS DECIMAL(20,0), 
            AVG_REVIEWS_RATING DECIMAL(20,0), 
            CONSTRAINT FACT_AIRBNB_AUSTIN_LA_PKEY PRIMARY KEY (LISTING_ID));
            """)

    load_fact_airbnb_austin_la_insert = ("""
        INSERT INTO FACT_AIRBNB_AUSTIN_LA (HOST_ID, LISTING_ID, NEIGHBOURHOOD_PIN, ADJUSTED_PRICE, NUMBER_OF_REVIEWS, AVG_REVIEWS_RATING)
        SELECT p.HOST_ID, p.LISTING_ID, p.NEIGHBOURHOOD_PIN, c.ADJUSTED_PRICE, COUNT(r.REVIEW_ID), AVG(r.REVIEW_SCORES_RATING) 
        FROM DIM_PROPERTIES p 
        INNER JOIN DIM_REVIEWS r on p.listing_id = r.listing_id 
        INNER JOIN DIM_CALENDARS c on p.listing_id = c.listing_id 
        group by p.host_id, p.listing_id, p.NEIGHBOURHOOD_PIN, c.ADJUSTED_PRICE
    """)

