import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql import SparkSession

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# sc = SparkContext()
# spark = glueContext.spark_session
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
# spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.sql.adaptive.enabled", "true")

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

days = 45
query =  f"""

WITH
  visit_info_nextgen AS (
    (
      SELECT
        datetime_est 
      , visitor_ip
      , visit_id
      , url
      , (case when date(datetime_est)<=date('2023-06-30') then (CASE WHEN ((lower(url) LIKE 'https://online.factoryoutletstore.com%') OR (lower(url) LIKE '%.factoryoutletstoreonline.com%')) THEN 'NextGen' else 'Legacy' END) else (case when lower(url) like '%www.factoryoutletstore.com%' and not rlike(lower(url),'/cat/|/catnc/|/details/|/website|/shoppingcart|/package|/category|/account|/help') then 'NextGen' else 'Legacy' end) end) site
      FROM
        web_analytics_fnd.visit_events
     WHERE date(datetime_est) >= date_sub(current_date(), {days})
   ) 
   UNION 
   (
      SELECT
        datetime_est 
      , visitor_ip
      , visit_id
      , url
      , (case when date(datetime_est)<=date('2023-06-30') then (CASE WHEN ((lower(url) LIKE 'https://online.factoryoutletstore.com%') OR (lower(url) LIKE '%.factoryoutletstoreonline.com%')) THEN 'NextGen' else 'Legacy' END) else (case when lower(url) like '%www.factoryoutletstore.com%' and not rlike(lower(url),'/cat/|/catnc/|/details/|/website|/shoppingcart|/package|/category|/account|/help') then 'NextGen' else 'Legacy' end) end) site
      FROM
      web_analytics_fnd.visit_actions
    WHERE date(datetime_est) >= date_sub(current_date(), {days})
   ) 
) 
, final_res_nextgen AS (
    select  
     ed.datetime_est 
   , ed.visitor_ip
   , ed.visit_id
   , ed.url 
   , ed.site
   , v.campaign_source
   , v.campaign_medium
   , v.ad_provider_id
   , v.referrer_type
   , v.referrer_name
      from (
     SELECT
     *,row_number() OVER (PARTITION BY visit_id ORDER BY datetime_est ASC) rank
     FROM
     visit_info_nextgen
     ) ed
     left join web_analytics_fnd.visits v on v.visit_id = ed.visit_id 
    WHERE (rank = 1)
)
, abandoned_cart_visits AS (
   SELECT
     veao.visit_id
   , veao.visitor_ip
   , veao.item_sku
   , vj.url
   FROM
     ((
      SELECT
        visit_id
      , visitor_ip
      , item_sku
      , row_number() OVER (PARTITION BY visitor_ip, item_sku ORDER BY datetime_est DESC) rank
      FROM
        web_analytics_fnd.visit_ecommerce_abandoned_cart_item_details
   )  veao
   LEFT JOIN final_res_nextgen vj ON (vj.visit_id = veao.visit_id))
   WHERE (veao.rank = 1)
) 
, untracked_order AS (
   SELECT DISTINCT
     date(datetime_est)
   , ed.visit_id
   , max(ac.url) url
   , v.campaign_source
   , v.campaign_medium
   , v.ad_provider_id
   , v.referrer_type
   , v.referrer_name
   FROM
     ((((final_res_nextgen ed
   INNER JOIN (
      SELECT visit_id
      FROM
        web_analytics_fnd.visit_goals
   )  vg ON (vg.visit_id = ed.visit_id))
   LEFT JOIN (
      SELECT
        visit_id
      , item_sku
      FROM
        web_analytics_fnd.visit_ecommerce_order_item_details
   )  veoi ON (veoi.visit_id = ed.visit_id))
   INNER JOIN abandoned_cart_visits ac ON ((ac.visitor_ip = ed.visitor_ip) AND (lower(ac.item_sku) = veoi.item_sku)))
   LEFT JOIN web_analytics_fnd.visits v ON (v.visit_id = ac.visit_id))
   WHERE ((ed.referrer_type <> 'campaign') AND (NOT (lower(ed.url) LIKE '%campaign%')) AND (NOT (lower(ed.url) LIKE '%chid%')) AND (NOT (lower(ed.url) LIKE '%utm_source%')) AND (ac.url IS NOT NULL))
   GROUP BY date(datetime_est),ed.visit_id,v.campaign_source, v.campaign_medium, v.ad_provider_id, v.referrer_type, v.referrer_name
) 
, end_data AS (
   SELECT DISTINCT
     ed.visit_id
   , ed.visitor_ip
   , ed.datetime_est
   , ed.site
   , COALESCE(uo.url, ed.url) url
   , COALESCE(uo.campaign_source, ed.campaign_source) campaign_source
   , COALESCE(uo.campaign_medium, ed.campaign_medium) campaign_medium
   , COALESCE(uo.ad_provider_id, ed.ad_provider_id) ad_provider_id
   , COALESCE(uo.referrer_type, ed.referrer_type) referrer_type
   , COALESCE(uo.referrer_name, ed.referrer_name) referrer_name
   FROM
     (final_res_nextgen ed
   LEFT JOIN untracked_order uo ON (uo.visit_id = ed.visit_id))
) 
, f_data as (
    select  
      visit_id
    , visitor_ip
    , datetime_est
    , site
    , url
    , regexp_extract(url, '(?<=campaignid)\=?([0-9]+)') campaign_id
    , regexp_extract(url, '(?<=cid|utm_cid|product_id)\=?([A-Za-z0-9]+)') mms_campaign_id
    , regexp_extract(url, '(utm_source=([a-z]+))', 2) utm_source
    , regexp_extract(url, '(?<=chid)\=?([0-9]+)') mms_channel_id
    , regexp_extract(url, '(?<=adgroupid)\=?([0-9]+)') adgroup_id
    , regexp_extract(url, '(kwd\-([0-9]+))', 2) keyword_id
    , lower(COALESCE(regexp_extract(url, '(?<=product_id)\=?([A-Za-z0-9]+)'), (CASE WHEN ((url LIKE '%adtype=pla%') OR (url LIKE '%targetid=pla-%')) THEN regexp_extract(url, '(?<=cid|utm_cid)\=?([A-Za-z0-9]+)') ELSE null END))) product_id
    , regexp_extract(url, '(?<=creative|adid)\=?([0-9]+)') creative_id
    , (CASE WHEN (lower(url) LIKE '%dsa-%' or lower(url) LIKE '%dat-%') THEN true ELSE false END) is_dsa_ad
    , campaign_source
    , campaign_medium
    , ad_provider_id
    , referrer_type
    , referrer_name
    from end_data
) 
, visit_sales AS (
   SELECT
   veo.datetime_est report_datetime
   , veo.visit_id
   , (element_at(split(veo.order_id,'-'), 3)) demand_order_id
   , pr.order_id
   , pr.order_status
   , pr.order_payment_status
   , pr.demand_revenue
   , pr.net_revenue
   , pr.return_revenue
   , pr.cancelled_revenue
   , pr.demand_quantity
   , pr.net_quantity
   , pr.return_quantity
   , pr.cancelled_quantity
   , pr.demand_profit
   , pr.net_profit
   , pr.shipping_charged
   , pr.tax_collected
   , pr.staff_owner_contact_id
   , pr.total_item_cost
   , pr.selling_price
   , pr.coupon_applied
   , pr.coupon_discount
   , (case when (cancelled_order_id is not null) or (cancelled_order_id <> '') then 1 end) cancel_orders 
   , (CASE WHEN (order_payment_status IN ('PAID', 'PARTIALLY_PAID')) THEN false WHEN ((order_payment_status = 'AUTHORIZED') AND (order_status IN ('Cancelled', 'Issue Refund'))) THEN true ELSE null END) is_order_cancelled
   FROM
     (web_analytics_fnd.visit_ecommerce_orders veo
   LEFT JOIN 
   (select * from nextgen_reporting.profit_loss_report  
   where date(order_created_date) >= date_sub(current_date(), {days})) pr ON (element_at(split(veo.order_id,'-'), 3)) = pr.order_id)
   where date(veo.datetime_est) >= date_sub(current_date(), {days})
) 
, extracted_visits AS (
   SELECT
     COALESCE(ed.datetime_est,vs.report_datetime) report_datetime
    , COALESCE(vs.visit_id,ed.visit_id) visit_id
    , ed.url
    , ed.site
    , ed.campaign_id
    , ed.mms_campaign_id
    , ed.mms_channel_id
    , ed.utm_source
    , ed.adgroup_id
    , ed.keyword_id
    , ed.product_id
    , ed.creative_id
    , ed.is_dsa_ad
    , ed.campaign_source
    , ed.campaign_medium
    , ed.ad_provider_id
    , ed.referrer_type
    , ed.referrer_name
    , demand_order_id
    , vs.order_id
    , vs.order_status
    , vs.order_payment_status
    , vs.demand_revenue
    , vs.net_revenue
    , vs.return_revenue
    , vs.cancelled_revenue
    , vs.demand_quantity
    , vs.net_quantity
    , vs.return_quantity
    , vs.cancelled_quantity
    , vs.demand_profit
    , vs.net_profit
    , vs.shipping_charged
    , vs.tax_collected
    , vs.staff_owner_contact_id
    , vs.total_item_cost
    , vs.selling_price
    , vs.coupon_applied
    , vs.coupon_discount
    , vs.cancel_orders
    , vs.is_order_cancelled
   FROM
     (f_data ed
   FULL JOIN visit_sales vs ON (ed.visit_id = vs.visit_id))
) 
, final_v AS (
   
   SELECT
    report_datetime date_est
   , ev.visit_id
   , ev.url
   , ev.site
   , ev.campaign_id
   , ev.mms_campaign_id
   , (CASE WHEN ((ev.mms_channel_id IS NULL) OR (ev.mms_channel_id = '')) THEN (CASE WHEN (lower(ev.campaign_source) IN ('google pla', 'google plautm_medium=cpc', 'google?utm_medium=pla', 'google p', 'google%20pla')) THEN '4272' WHEN (((lower(ev.campaign_source) IN ('google')) AND (ev.url LIKE '%pla-%')) OR ((lower(ev.ad_provider_id) = lower('google')) AND (lower(ev.campaign_medium) = 'cpc'))) THEN '4272' WHEN (lower(ev.campaign_source) IN ('bing pla', 'bingpla', 'bing plautm_medium=cpc')) THEN '4282' WHEN (((lower(ev.campaign_source) IN ('bing')) AND (url LIKE '%pla-%')) OR ((lower(ev.ad_provider_id) = lower('bing')) AND (lower(ev.campaign_medium) = 'cpc'))) THEN '4282' WHEN ((lower(ev.campaign_source) IN ('google text accesory for', 'google%20text%20general', 'google%20text%20models', 'google text models', 'google text general', 'google text generic', 'google text accessory for', 'google text accessory%for')) OR ((lower(ev.ad_provider_id) = lower('google')) AND (lower(ev.campaign_medium) = 'text'))) THEN '1' WHEN ((lower(ev.campaign_source) IN ('bing text accessory for', 'bing text general', 'bing text generic', 'bing text accessory%for', 'bing text models')) OR ((lower(ev.ad_provider_id) = lower('bing')) AND (lower(ev.campaign_medium) = 'text'))) THEN '1600' WHEN ((COALESCE(ev.campaign_source, ev.utm_source) = 'mailmodo') OR (lower(ev.campaign_source) IN ('mailmodo', 'mailm', '3dmailmodo'))) THEN '4'  WHEN ((lower(ev.campaign_source) IN ('facebook', 'social-fb', 'facebookads')) OR (lower(ev.referrer_name) = lower('facebook')) OR (lower(ev.ad_provider_id) = 'facebook')) THEN '4292'  WHEN rlike(lower(ev.url),'cjevent|mms_chref=cj') THEN 2100 WHEN (lower(ev.referrer_name) = 'pinterest') THEN '4275' WHEN (LOWER(ev.referrer_type) = 'direct') THEN '0' END) ELSE ev.mms_channel_id END) mms_channel_id
   , ev.adgroup_id
   , ev.keyword_id
   , ev.product_id
   , ev.creative_id
   , ev.is_dsa_ad
   , (CASE WHEN (lower(ev.campaign_source) = 'google display') THEN true ELSE false END) is_google_display_ad
   , ev.demand_order_id
   , ev.order_id
   , ev.order_status
   , ev.order_payment_status
   , ev.demand_revenue
   , ev.net_revenue
   , ev.return_revenue
   , ev.cancelled_revenue
   , ev.demand_quantity
   , ev.net_quantity
   , ev.return_quantity
   , ev.cancelled_quantity
   , ev.demand_profit
   , ev.net_profit
   , ev.shipping_charged
   , ev.tax_collected
   , ev.staff_owner_contact_id
   , ev.total_item_cost
   , ev.selling_price
   , ev.coupon_applied
   , ev.coupon_discount
   , ev.cancel_orders
   , ev.is_order_cancelled
   , av.site_id
   , av.site_name
   , 69 sub_store_id
   , 69 store_id
   , av.visitor_ip
   , av.visitor_id
   , av.first_action_datetime_utc
   , av.first_action_datetime_est
   , av.last_action_datetime_utc
   , av.last_action_datetime_est
   , av.fingerprint
   , av.goal_conversions
   , av.site_currency
   , av.site_currency_symbol
   , av.visit_server_hour_utc visit_server_hour
   , av.user_id
   , av.visitor_type
   , av.visitor_type_icon
   , av.visit_converted
   , av.visit_converted_icon
   , av.visit_count
   , av.visit_ecommerce_status
   , av.visit_ecommerce_status_icon
   , av.days_since_first_visit
   , av.seconds_since_first_visit
   , av.days_since_last_ecommerce_order
   , av.seconds_since_last_ecommerce_order
   , av.visit_duration
   , av.searches
   , av.actions
   , av.interactions
   , ev.referrer_type
   , av.referrer_type_name
   , ev.referrer_name
   , av.referrer_keyword
   , av.referrer_keyword_position
   , av.referrer_url
   , av.referrer_search_engine_url
   , av.referrer_search_engine_icon
   , av.referrer_social_network_url
   , av.referrer_social_network_icon
   , av.language_code
   , av.language
   , av.device_type
   , av.device_type_icon
   , av.device_brand
   , av.device_model
   , av.operating_system
   , av.operating_system_name
   , av.operating_system_icon
   , av.operating_system_code
   , av.operating_system_version
   , av.browser_family
   , av.browser_family_description
   , av.browser
   , av.browser_name
   , av.browser_icon
   , av.browser_code
   , av.browser_version
   , av.total_ecommerce_revenue
   , av.total_ecommerce_conversions
   , av.total_ecommerce_items
   , av.total_abandoned_carts_revenue
   , av.total_abandoned_carts
   , av.total_abandoned_carts_items
   , av.events
   , av.continent
   , av.continent_code
   , av.country
   , av.country_code
   , av.country_flag
   , av.region
   , av.region_code
   , av.city
   , av.location
   , av.latitude
   , av.longitude
   , av.visit_local_time
   , av.visit_local_hour
   , av.days_since_last_visit
   , av.seconds_since_last_visit
   , av.resolution
   , av.plugins
   , av.ad_click_id
   , ev.ad_provider_id
   , av.ad_provider_name
   , av.ad_provider_icon
   , av.form_conversions
   , av.session_replay_url
   , av.campaign_id campaign_id_matomo
   , av.campaign_content
   , av.campaign_keyword
   , ev.campaign_medium
   , av.campaign_name campaign_name_matomo
   , ev.campaign_source
   , av.campaign_group
   , av.campaign_placement
   , '' site_url
   , av.first_action_timestamp_utc first_action_timestamp
   , av.last_action_timestamp_utc last_action_timestamp
   , av.first_action_date_est
   FROM
     (extracted_visits ev
   FULL JOIN web_analytics_fnd.visits av ON (av.visit_id = ev.visit_id))
   where date(av.first_action_datetime_est) >= date_sub(current_date(), {days})
) 

SELECT 
    date_est,
    visit_id,
    url,
    site,
    campaign_id,
    mms_campaign_id,
    mms_channel_id,
    (
    	CASE
    		WHEN (mms_channel_id = '4') THEN 'Mailmodo'
    		WHEN (mms_channel_id = '1') THEN 'Google Text'
    		WHEN (mms_channel_id = '1600') THEN 'Bing Text'
    		WHEN (mms_channel_id = '4282') THEN 'Bing PLA'
    		WHEN (mms_channel_id = '4272') THEN 'Google PLA'
    		WHEN (mms_channel_id = '4274') THEN 'Google Remarketing'
    		WHEN (mms_channel_id = '4275') THEN 'Pinterest'
    		WHEN (mms_channel_id = '4292') THEN 'Facebook'
    		WHEN (mms_channel_id = '2100') THEN 'Commission Junction'
    		WHEN (mms_channel_id = '0') THEN 'Organic' ELSE 'Unknown'
    	END
    ) channel,
    adgroup_id,
    keyword_id,
    product_id,
    creative_id,
    is_dsa_ad,
    is_google_display_ad,
    demand_order_id,
    order_id,
    order_status,
    order_payment_status,
    demand_revenue,
    net_revenue,
    return_revenue,
    cancelled_revenue,
    demand_quantity,
    net_quantity,
    return_quantity,
    cancelled_quantity,
    demand_profit,
    net_profit,
    shipping_charged,
    tax_collected,
    staff_owner_contact_id,
    total_item_cost,
    selling_price,
    coupon_applied,
    coupon_discount,
    cancel_orders,
    is_order_cancelled,
    site_id,
    site_name,
    sub_store_id,
    store_id,
    visitor_ip,
    visitor_id,
    first_action_datetime_utc,
    first_action_datetime_est,
    last_action_datetime_utc,
    last_action_datetime_est,
    fingerprint,
    goal_conversions,
    site_currency,
    site_currency_symbol,
    visit_server_hour,
    user_id,
    visitor_type,
    visitor_type_icon,
    visit_converted,
    visit_converted_icon,
    visit_count,
    visit_ecommerce_status,
    visit_ecommerce_status_icon,
    days_since_first_visit,
    seconds_since_first_visit,
    days_since_last_ecommerce_order,
    seconds_since_last_ecommerce_order,
    visit_duration,
    searches,
    actions,
    interactions,
    referrer_type,
    referrer_type_name,
    referrer_name,
    referrer_keyword,
    referrer_keyword_position,
    referrer_url,
    referrer_search_engine_url,
    referrer_search_engine_icon,
    referrer_social_network_url,
    referrer_social_network_icon,
    language_code,
    language,
    device_type,
    device_type_icon,
    device_brand,
    device_model,
    operating_system,
    operating_system_name,
    operating_system_icon,
    operating_system_code,
    operating_system_version,
    browser_family,
    browser_family_description,
    browser,
    browser_name,
    browser_icon,
    browser_code,
    browser_version,
    total_ecommerce_revenue,
    total_ecommerce_conversions,
    total_ecommerce_items,
    total_abandoned_carts_revenue,
    total_abandoned_carts,
    total_abandoned_carts_items,
    events,
    continent,
    continent_code,
    country,
    country_code,
    country_flag,
    region,
    region_code,
    city,
    location,
    latitude,
    longitude,
    visit_local_time,
    visit_local_hour,
    days_since_last_visit,
    seconds_since_last_visit,
    resolution,
    plugins,
    ad_click_id,
    ad_provider_id,
    ad_provider_name,
    ad_provider_icon,
    form_conversions,
    session_replay_url,
    campaign_id_matomo,
    campaign_content,
    campaign_keyword,
    campaign_medium,
    campaign_name_matomo,
    campaign_source,
    campaign_group,
    campaign_placement,
    site_url,
    first_action_timestamp,
    last_action_timestamp,
    first_action_date_est
FROM
  final_v
"""
df = spark.sql(query)

df.write.format('delta')  \
    .partitionBy('first_action_date_est') \
    .option('mergeSchema','true')\
    .mode('overwrite') \
    .save('s3://gogotech-nextgen-datalake/reporting/visits_to_marketing_performance_nextgen/') \


# df.repartition('first_action_date') \
#         .write.format('parquet')  \
#         .option('path', f's3://gogotech-nextgen-datalake/reporting/visits_to_marketing_performance_nextgen/') \
#         .mode('overwrite') \
#         .insertInto("nextgen_reporting.visits_to_marketing_performance_nextgen")
job.commit()