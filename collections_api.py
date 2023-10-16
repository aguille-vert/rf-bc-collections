import jmespath
import requests

def add_request_array_to_collection(request_array,
                                          collection_id,
                                          api_keys,
                                          api_service
                                          ):

  
  if len(request_array)<=1000:
    r = put_request_array_to_collection(request_array,
                                   collection_id,
                                   api_keys,
                                    api_service)
  else:
    n_batches = len(request_array)//1000
    count = 0
    n=1000
    for _ in range(n_batches):
      request_array_batch = request_array[count:count+n]
      r = put_request_array_to_collection(request_array_batch,
                                   collection_id,
                                   api_keys,
                                    api_service)
      count+=n
    # remainder
    request_array_batch = request_array[count:]
    r = put_request_array_to_collection(request_array_batch,
                                   collection_id,
                                   api_keys,
                                    api_service)
  return r


def clear_collection(collection_id,
                     api_service,
                     api_keys):
  api_key = api_keys[api_service]
  
  url=f"https://api.{api_service}api.com/collections/{collection_id}/clear?api_key={api_key}"
  
  return requests.delete(url)


def create_api_collection_from_(collection_name,
                            destination_ids,
                            api_keys,
                          api_service,
                            notification_email = None,
                            schedule_type = None,
                            schedule_hours=None,
                            priority="normal"):
  """
  api_key, name and destination_ids are required Params
  """

  api_key = api_keys[api_service]
  
  if schedule_type:
    body = {
      "name": collection_name,
      "enabled": True,
      "schedule_type": schedule_type,
      "priority": priority,
      "schedule_hours": schedule_hours,
      "destination_ids": destination_ids,
      "notification_email": notification_email,
      "notification_as_json": True
    }
  else:
    body = {
        "name": collection_name,
        "destination_ids": destination_ids,
        "notification_email": notification_email,
        "notification_as_json": True
    }
  url = f'https://api.{api_service}api.com/collections?api_key={api_key}'
  api_result = requests.post(url, json=body)

  api_response = api_result.json()

  return api_response

def create_array_of_rainforest_requests(request_type,
                                        amazon_domain,
                                        items=None,
                                        item_type = 'asin',
                                        offer_ids=None,
                                        search_terms = None,
                                        category_id=None,
                                        num_pages=None):
  
  """
  items: asins or gtins; item_type: asin or gtin
  """

  if request_type == 'product':
    return [{"type": request_type,
      "amazon_domain": amazon_domain,
      "offers_condition_new":"true",
      item_type: i} for i in items]

  if request_type == 'offers':
    item_type='asin'
    return [{"type": request_type,
      "amazon_domain": amazon_domain,
      "offers_condition_new":"true",
      "asin": i} for i in items]

  if request_type == "stock_estimation":
    error_message="lengths of asins and offer_ids should be equal"
    assert len(items)==len(offer_ids), print(error_message)

  if request_type == 'search':
    return [{"type" : request_type,
            "amazon_domain" : amazon_domain,
            "search_term" : search_term} for search_term in search_terms]
  
  if request_type == 'category':
    return[{"type" : request_type,
            'category_id' : category_id,
            'amazon_domain' : amazon_domain,
            'page':page_num} for page_num in range(1,num_pages+1)]

    # return [{"type": request_type,
    #   "amazon_domain": amazon_domain, 
    #   "asin": asin,
    #   "offer_id" : offer_id} for asin, offer_id in zip(items,offer_ids)]

def create_bluecart_request_array_from_(request_type,
                              request_items,
                              max_page=None
                              ):
  """
  for type == 'search' request_items are strings, such as amz_titles
  for type == 'product' request_items are wlm_item_ids, max_page=None
  """
  
  if request_type == "search":
    request_array =  [
        {
            "type": request_type,
            "max_page": max_page, 
            "search_term": i
         } 
      for i in request_items]

  if request_type in ['product','offers']:
        request_array =  [
            {
                "type": request_type,
                "item_id": i
             } 
            for i in request_items]

  if request_type == 'seller_profile':
    request_array =  [
            {
                "type": request_type,
                "seller_id": i
             } 
            for i in request_items]




          
  return request_array

def delete_collection_(collection_id,
                                  api_keys,
                                  api_service):
  api_key = api_keys[api_service]
  base=f"https://api.{api_service}api.com/collections"
  url=f"{base}/{collection_id}?api_key={api_key}"
  print(url)
  return requests.delete(url)


def parse_amz_json_from_(data):
  res=[]
  try:
    if 'result' in list(data):
      pr = data['result']['product']
    else:
      pr=data
    max_order_quantity_expr = jmespath.compile('buybox_winner.maximum_order_quantity.value')
    new_offers_count_expr=jmespath.compile('buybox_winner.new_offers_count')
    availability_type_expr = jmespath.compile('buybox_winner.availability.type')
    price_value_expr = jmespath.compile('buybox_winner.price.value')
    is_prime_expr = jmespath.compile('buybox_winner.is_prime')
    is_new_expr = jmespath.compile('buybox_winner.condition.is_new')
    dispatch_days_expr = jmespath.compile('buybox_winner.availability.dispatch_days')
    fulfillment_type_expr = jmespath.compile('buybox_winner.fulfillment.type')
    amazon_seller_expr = jmespath.compile('buybox_winner.fulfillment.amazon_seller.name')
    fba_expr = jmespath.compile('buybox_winner.fulfillment.is_fulfilled_by_amazon')
    shipping_cost_expr = jmespath.compile('buybox_winner.shipping.raw')
    
    fields = cols[:-11]
    for name in fields:

      try:
        i = pr[name]
      except:
        i=None
      res.append(i)

    for expr in [max_order_quantity_expr,
                  new_offers_count_expr,
                  availability_type_expr,
                  price_value_expr,
                is_prime_expr,
                is_new_expr,
                dispatch_days_expr,
                fulfillment_type_expr,
                amazon_seller_expr,
                fba_expr,
                shipping_cost_expr]:

      res.append(expr.search(pr))
    
  
    return res
  except:
    pass


def put_request_array_to_collection(request_array,
                                   collection_id,
                                   api_keys,
                                    api_service):
  api_key = api_keys[api_service]
  base = f"https://api.{api_service}api.com/collections"
  url = f"{base}/{collection_id}?api_key={api_key}"
  body = { 
          "requests": request_array
          }
  api_result = requests.put(url, json=body)
  return  api_result.json()

def start_collection(collection_id,
                                api_keys,
                                api_service):
  api_key = api_keys[api_service]
  params = {
    'api_key': api_key
  }
  base = f"https://api.{api_service}api.com/collections"
  url = f"{base}/{collection_id}/start"

  api_result = requests.get(url, params)

  return api_result.json()

cols = ['item_volume',
 'add_an_accessory',
 'feature_bullets_flat',
 'specifications',
 'attributes',
 'editorial_reviews',
 'parent_asin',
 'a_plus_content',
 'has_360_view',
 'specifications_flat',
 'sell_on_amazon',
 'rating',
 'brand_image',
 'editorial_reviews_flat',
 'keywords',
 'series',
 'genres',
 'rich_product_description',
 'videos_additional',
 'dimensions',
 'whats_in_the_box',
 'additional_details',
 'ratings_total',
 'reviews_total',
 'amazons_choice',
 'bestseller_badge',
 'documents',
 'important_information',
 'hardcover',
 'videos_count',
 'has_size_guide',
 'manufacturer',
 'images',
 'authors',
 'link',
 'categories',
 'color',
 'is_bundle',
 'sub_title',
 'isbn_10',
 'buying_options',
 'variants_message',
 'format',
 'proposition_65_warning',
 'energy_efficiency',
 'label',
 'size_guide_html',
 'title_excluding_variant_name',
 'material',
 'starring',
 'protection_plans',
 'bestsellers_rank_flat',
 'isbn_13',
 'asin',
 'buybox_winner',
 'publication_date',
 'has_coupon',
 'language',
 'marketplace_id',
 'brand',
 'promotions_feature',
 'image_overlay_badge',
 'additional_details_flat',
 'prime_video_rating',
 'more_buying_choices',
 'book_description',
 'gtin',
 'categories_flat',
 'search_alias',
 'platform',
 'prime_video_xray',
 'model_number',
 'release_date',
 'main_image',
 'publisher',
 'bestsellers_rank',
 'feature_bullets',
 'coupon_text',
 'first_available',
 'weight',
 'variants',
 'videos',
 'recommended_age',
 'pages',
 'kindle_unlimited',
 'feature_bullets_count',
 'description',
 'title',
 'images_flat',
 'rating_breakdown',
 'keywords_list',
 'variant_asins_flat',
 'images_count',
 'prime_video_subtitles',
 'top_reviews',
 'videos_flat',
 'max_order_quantity',
 'new_offers_count',
 'availability_type',
 'price',
 'is_prime',
 'is_new',
 'dispatch_days',
 'fulfillment_type',
 'amazon_seller',
 'fba',
 'shipping_cost']