import jmespath


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