import awswrangler as wr
import boto3
import numpy as np
import pandas as pd
import sys
from pymysql import cursors, connect
import datetime
import pytz
from sqlalchemy import create_engine
from json import loads
from botocore.exceptions import ClientError

session = boto3.Session(region_name='us-east-1')
secrets_client = session.client(
    service_name='secretsmanager',
    region_name="us-east-1")
    
eastern = pytz.timezone('US/Eastern')

def get_secret(secretname):
    """this function is for get secrets of database from aws secretsmanager"""
    try:
        get_secret_value_response = secrets_client.get_secret_value(
            SecretId=secretname
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    secret = get_secret_value_response['SecretString']
    return secret



secretarn = "arn:aws:secretsmanager:us-east-1:578236606604:secret:prod/data-resources-db-mysql-kFOx3p"
secrets = loads(get_secret(secretarn))

endpoint = secrets.get("db_host")
username = secrets.get("db_username")
password = secrets.get("db_password")
database_name = 'gogotech_nextgen'
table_name = 'akeneo_sku_wise_pageviews'

print(f"mysql+pymysql://{username}:{password}@{endpoint}/{database_name}")

engine = create_engine(
    f"mysql+pymysql://{username}:{password}@{endpoint}/{database_name}"
)
try:
    conn = connect(host=endpoint, user=username,
                   passwd=password, db=database_name,
                   cursorclass=cursors.DictCursor)
except Exception as e:
    print(e)
    sys.exit()
boto3_session = boto3.Session(
    region_name='us-east-1'
)

query = """
with impressions_data as (
	with text_data as (
		with text_impressions as (
			select site,
				split_part(lower(gbt."ad_final_url"), '?', 1) url,
				(
					case
						when regexp_like(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'details/[0-9]+-[0-9]+/'
						) then regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'/details/([0-9]+)-([0-9]+)/',
							2
						) else regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'details/([0-9]+)',
							1
						)
					end
				) container_id,
				regexp_extract(
					lower(gbt.ad_final_url),
					'category_id=([0-9]+)',
					1
				) category_id,
				coalesce(
					regexp_extract(
						lower(gbt.ad_final_url),
						'catalogitemid=([0-9]+)',
						1
					),
					cast(ctc."catalog_item_id" as varchar)
				) catalog_item_id,
				coalesce(
					(
						case
							when site = 'NextGen' then (
								case
									when split_part(
										reverse(
											lower(split_part(lower(gbt.ad_final_url), '?', 1))
										),
										'/',
										1
									) = '' then (
										case
											when split_part(
												split_part(
													reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
													'/',
													2
												),
												'-',
												1
											) in ('n', 'r', 'u', 'ob', 'mr', 'cr') then regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														2
													)
												),
												'([^/]+)-[nruobmc]$',
												1
											) else regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														2
													)
												),
												'([^/]+)$'
											)
										end
									) else (
										case
											when split_part(
												split_part(
													reverse(
														lower(split_part(lower(gbt.ad_final_url), '?', 1))
													),
													'/',
													1
												),
												'-',
												1
											) in ('n', 'r', 'u', 'ob', 'mr', 'cr') then regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														1
													)
												),
												'([^/]+)-[nruobmc]$',
												1
											) else regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														1
													)
												),
												'([^/]+)$'
											)
										end
									)
								end
							)
						end
					),
					nlm."akeneo_sku"
				) akeneo_sku,
				sum("impressions") impressions
			from "nextgen_reporting"."gogotech_google_bing_ads_ad_level_hourly" gbt
				left join "gogotech_launch"."sc_container_catalog_item_assignments" ctc on (
					case
						when regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'details/([0-9]+)',
							1
						) = '0' then regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'/details/([0-9]+)-([0-9]+)/',
							2
						) else regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'details/([0-9]+)',
							1
						)
					end
				) = cast(ctc."container_id" as varchar)
				left join "gogotech_nextgen"."nextgen_to_legacy_sku_mappings" nlm on cast(nlm."catalog_item_id" as varchar) = coalesce(
					regexp_extract(
						lower(gbt.ad_final_url),
						'catalogitemid=([0-9]+)',
						1
					),
					cast(ctc."catalog_item_id" as varchar)
				)
			where gbt."ad_final_url" is not null
				and regexp_like(
					lower(gbt."ad_final_url"),
					'(/details/|/product/)'
				)
				and date(date) >= date(
					date_add(
						'month',
						-4,
						date_trunc('day', current_timestamp) - interval '2' day
					)
				)
			group by site,
				split_part(lower(gbt."ad_final_url"), '?', 1),
				(
					case
						when regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'details/([0-9]+)',
							1
						) = '0' then regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'/details/([0-9]+)-([0-9]+)/',
							2
						) else regexp_extract(
							split_part(lower(gbt.ad_final_url), '?', 1),
							'details/([0-9]+)',
							1
						)
					end
				),
				regexp_extract(
					lower(gbt.ad_final_url),
					'category_id=([0-9]+)',
					1
				),
				coalesce(
					regexp_extract(
						lower(gbt.ad_final_url),
						'catalogitemid=([0-9]+)',
						1
					),
					cast(ctc."catalog_item_id" as varchar)
				),
				coalesce(
					(
						case
							when site = 'NextGen' then (
								case
									when split_part(
										reverse(
											lower(split_part(lower(gbt.ad_final_url), '?', 1))
										),
										'/',
										1
									) = '' then (
										case
											when split_part(
												split_part(
													reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
													'/',
													2
												),
												'-',
												1
											) in ('n', 'r', 'u', 'ob', 'mr', 'cr') then regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														2
													)
												),
												'([^/]+)-[nruobmc]$',
												1
											) else regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														2
													)
												),
												'([^/]+)$'
											)
										end
									) else (
										case
											when split_part(
												split_part(
													reverse(
														lower(split_part(lower(gbt.ad_final_url), '?', 1))
													),
													'/',
													1
												),
												'-',
												1
											) in ('n', 'r', 'u', 'ob', 'mr', 'cr') then regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														1
													)
												),
												'([^/]+)-[nruobmc]$',
												1
											) else regexp_extract(
												reverse(
													split_part(
														reverse(split_part(lower(gbt.ad_final_url), '?', 1)),
														'/',
														1
													)
												),
												'([^/]+)$'
											)
										end
									)
								end
							)
						end
					),
					nlm."akeneo_sku"
				)
		)
		select akeneo_sku,
			sum(impressions) impressions
		from text_impressions
		where akeneo_sku is not null
		group by akeneo_sku
	),
	pla_data as (
		with pla_impressions as (
			SELECT site,
				(
					case
						when site = 'NextGen' then regexp_extract(lower(product_item_id), 'ng([0-9]+)', 1)
					end
				) brightpearl_id,
				tc.catalogitemid catalog_item_id,
				nlm.akeneo_sku,
				sum("impressions") impressions
			FROM "nextgen_reporting"."gogotech_google_bing_ads_pla_hourly" gbp
				left join "gogotech_reporting"."t_campaign" tc on (
					case
						when gbp.site = 'Legacy' then gbp.product_item_id
					end
				) = cast(tc."campaignid" as varchar)
				left join "gogotech_nextgen"."nextgen_to_legacy_sku_mappings" nlm on cast(nlm."catalog_item_id" as varchar) = cast(tc.catalogitemid as varchar)
			where date(gbp.date) >= date(
					date_add(
						'month',
						-4,
						date_trunc('day', current_timestamp) - interval '2' day
					)
				)
				and (
					tc."catalogitemid" is not null
					or regexp_extract(lower(product_item_id), 'ng([0-9]+)', 1) is not null
				)
			group by gbp.site,
				(
					case
						when site = 'NextGen' then regexp_extract(lower(product_item_id), 'ng([0-9]+)', 1)
					end
				),
				nlm.akeneo_sku,
				tc."catalogitemid"
		)
		select coalesce(pi."akeneo_sku", p."akeneo_sku") akeneo_sku,
			sum(pi."impressions") impressions
		from pla_impressions pi
			left join "products_fnd"."products" p on p.brightpearl_id = pi."brightpearl_id"
		where coalesce(pi."akeneo_sku", p."akeneo_sku") is not null
		group by coalesce(pi."akeneo_sku", p."akeneo_sku")
	)
	select lower(akeneo_sku) akeneo_sku,
		sum(impressions) impressions
	from (
			select *
			from text_data
			union
			select *
			from pla_data
		)
	group by lower(akeneo_sku)
),
data as (
	select va.visit_id,
		split_part(
			lower(
				replace(replace(replace(url, '€', ''), '', ''), 'â', '')
			),
			'?',
			1
		) eurl,
		url,
		(
			case
				when site_id in (69, 71) then 'NextGen' else 'Legacy'
			end
		) site,
		(
			case
				when regexp_like(
					split_part(
						lower(
							replace(replace(replace(url, '€', ''), '', ''), 'â', '')
						),
						'?',
						1
					),
					'/details/[0-9]+-[0-9]+/'
				) then regexp_extract(
					split_part(
						lower(
							replace(replace(replace(url, '€', ''), '', ''), 'â', '')
						),
						'?',
						1
					),
					'/details/([0-9]+)-([0-9]+)/',
					2
				) else regexp_extract(
					split_part(
						lower(
							replace(replace(replace(url, '€', ''), '', ''), 'â', '')
						),
						'?',
						1
					),
					'details/([0-9]+)',
					1
				)
			end
		) container_id,
		regexp_extract(lower(url), 'category_id=([0-9]+)', 1) category_id,
		regexp_extract(lower(url), 'catalogitemid=([0-9]+)', 1) catalog_item_id,
		(
			case
				when site_id in (69, 71) then (
					case
						when split_part(
							reverse(
								lower(
									split_part(
										lower(
											replace(replace(replace(url, '€', ''), '', ''), 'â', '')
										),
										'?',
										1
									)
								)
							),
							'/',
							1
						) = '' then (
							case
								when split_part(
									split_part(
										reverse(
											split_part(
												lower(
													replace(replace(replace(url, '€', ''), '', ''), 'â', '')
												),
												'?',
												1
											)
										),
										'/',
										2
									),
									'-',
									1
								) in ('n', 'r') then regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											2
										)
									),
									'([^/]+)-[nr]$',
									1
								) else regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											2
										)
									),
									'([^/]+)$'
								)
							end
						) else (
							case
								when split_part(
									split_part(
										reverse(
											lower(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											)
										),
										'/',
										1
									),
									'-',
									1
								) in ('n', 'r', 'N', 'R') then regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											1
										)
									),
									'([^/]+)-[nr]$',
									1
								) else regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											1
										)
									),
									'([^/]+)$'
								)
							end
						)
					end
				)
			end
		) akeneo_sku,
		(
			case
				when site_id not in (69, 71) then reverse(
					split_part(
						reverse(
							split_part(
								lower(
									replace(replace(replace(url, '€', ''), '', ''), 'â', '')
								),
								'?',
								1
							)
						),
						'/',
						1
					)
				)
			end
		) product_identifier,
		count(
			case
				when vs."is_order_cancelled" = True then vs."order_id"
			end
		) cancel_orders,
		count(
			case
				when vs."is_order_cancelled" = False then vs."order_id"
			end
		) net_orders,
		sum(
			case
				when vs."is_order_cancelled" = True then vs."revenue"
			end
		) cancel_revenue,
		sum(
			case
				when vs."is_order_cancelled" = False then vs."revenue"
			end
		) net_revenue,
		sum(vs.profit) profit
	from "gogotech_foundation"."visit_actions" va
		left join (
			select "visit_id",
				"order_id",
				"is_order_cancelled",
				"revenue",
				"profit"
			from "nextgen_reporting"."visits_to_marketing_performance_legacy"
			where date(date_est) >= date(
					date_add(
						'month',
						-4,
						date_trunc('day', current_timestamp) - interval '2' day
					)
				)
			union
			select "visit_id",
				"order_id",
				"is_order_cancelled",
				"net_revenue" + "return_revenue" + "cancelled_revenue" revenue,
				"demand_profit" profit
			from "nextgen_reporting"."visits_to_marketing_performance_nextgen"
			where date(date_est) >= date(
					date_add(
						'month',
						-4,
						date_trunc('day', current_timestamp) - interval '2' day
					)
				)
		) vs on vs.visit_id = va.visit_id
	where regexp_like(
			lower(url),
			'(/details/|/product/|/products/)'
		)
		and date(datetime_est) >= date(
			date_add(
				'month',
				-4,
				date_trunc('day', current_timestamp) - interval '2' day
			)
		)
		and site_id != 67
		and lower(url) not like '%searchterm=%'
	group by 1,
		split_part(
			lower(
				replace(replace(replace(url, '€', ''), '', ''), 'â', '')
			),
			'?',
			1
		),
		url,
		(
			case
				when site_id in (69, 71) then 'NextGen' else 'Legacy'
			end
		),
		(
			case
				when regexp_extract(
					split_part(
						lower(
							replace(replace(replace(url, '€', ''), '', ''), 'â', '')
						),
						'?',
						1
					),
					'details/([0-9]+)',
					1
				) = '0' then regexp_extract(
					split_part(
						lower(
							replace(replace(replace(url, '€', ''), '', ''), 'â', '')
						),
						'?',
						1
					),
					'/details/([0-9]+)-([0-9]+)/',
					2
				) else regexp_extract(
					split_part(
						lower(
							replace(replace(replace(url, '€', ''), '', ''), 'â', '')
						),
						'?',
						1
					),
					'details/([0-9]+)',
					1
				)
			end
		),
		regexp_extract(lower(url), 'category_id=([0-9]+)', 1),
		regexp_extract(lower(url), 'catalogitemid=([0-9]+)', 1),
		(
			case
				when site_id in (69, 71) then (
					case
						when split_part(
							reverse(
								lower(
									split_part(
										lower(
											replace(replace(replace(url, '€', ''), '', ''), 'â', '')
										),
										'?',
										1
									)
								)
							),
							'/',
							1
						) = '' then (
							case
								when split_part(
									split_part(
										reverse(
											split_part(
												lower(
													replace(replace(replace(url, '€', ''), '', ''), 'â', '')
												),
												'?',
												1
											)
										),
										'/',
										2
									),
									'-',
									1
								) in ('n', 'r') then regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											2
										)
									),
									'([^/]+)-[nr]$',
									1
								) else regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											2
										)
									),
									'([^/]+)$'
								)
							end
						) else (
							case
								when split_part(
									split_part(
										reverse(
											lower(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											)
										),
										'/',
										1
									),
									'-',
									1
								) in ('n', 'r', 'N', 'R') then regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											1
										)
									),
									'([^/]+)-[nr]$',
									1
								) else regexp_extract(
									reverse(
										split_part(
											reverse(
												split_part(
													lower(
														replace(replace(replace(url, '€', ''), '', ''), 'â', '')
													),
													'?',
													1
												)
											),
											'/',
											1
										)
									),
									'([^/]+)$'
								)
							end
						)
					end
				)
			end
		),
		(
			case
				when site_id not in (69, 71) then reverse(
					split_part(
						reverse(
							split_part(
								lower(
									replace(replace(replace(url, '€', ''), '', ''), 'â', '')
								),
								'?',
								1
							)
						),
						'/',
						1
					)
				)
			end
		)
),
dis_select as (
	select array_agg(d.eurl) eurl,
		coalesce(
			d."catalog_item_id",
			cast(ctc."catalog_item_id" as varchar)
		) catalog_item_id,
		replace(d.akeneo_sku, 'jpg', '') akeneo_sku,
		count(url) pageviews,
		sum(d."net_orders") net_orders,
		sum(d."cancel_orders") cancel_orders,
		sum(d."net_revenue") net_revenue,
		sum(d."cancel_revenue") cancel_revenue,
		sum(d."profit") profit
	from data d
		left join "gogotech_launch"."sc_container_catalog_item_assignments" ctc on cast(ctc."container_id" as varchar) = d."container_id"
	group by coalesce(
			d."catalog_item_id",
			cast(ctc."catalog_item_id" as varchar)
		),
		d.akeneo_sku
)
select lower(coalesce(p.akeneo_sku, nlm.akeneo_sku)) akeneo_sku,
	sum(ds."pageviews") pageviews,
	sum(ds."net_orders") net_orders,
	sum(ds."cancel_orders") cancel_orders,
	round(sum(ds."net_revenue"), 2) net_revenue,
	round(sum(ds."cancel_revenue"), 2) cancel_revenue,
	round(sum(ds."profit"), 2) profit,
	max(id."impressions") impressions
from dis_select ds
	left join "gogotech_nextgen"."nextgen_to_legacy_sku_mappings" nlm on cast(nlm."catalog_item_id" as varchar) = ds."catalog_item_id"
	left join "products_fnd"."products" p on lower(p.akeneo_sku) = lower(ds.akeneo_sku)
	left join impressions_data id on lower(coalesce(ds.akeneo_sku, nlm.akeneo_sku)) = lower(id.akeneo_sku)
where lower(coalesce(p.akeneo_sku, nlm.akeneo_sku)) is not null
	and not regexp_like(
		lower(element_at(ds.eurl, 1)),
		'(_exploit_dom_xss|onerror=|%20)'
	)
group by lower(coalesce(p.akeneo_sku, nlm.akeneo_sku))
"""

if __name__ == '__main__':
    mapping_df = wr.athena.read_sql_query(
        keep_files=False,
        sql=query,
        database="nextgen_reporting",
        boto3_session=boto3_session
    )
    mapping_df = mapping_df.fillna(0)
    
    mapping_df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    conn.close()