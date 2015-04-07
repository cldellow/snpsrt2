DROP TABLE IF EXISTS products, listings, canonical_mfr, results;

CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TEMPORARY TABLE IF NOT EXISTS json(doc json);

CREATE TABLE products(
  id serial primary key,
  name citext not null,
  mfr citext not null,
  model citext not null,
  family citext,
  clean_product citext);

CREATE TABLE listings(
  title citext not null,
  mfr citext not null,
  clean_listing citext,
  -- note: we don't use currency/price, but they are required when producing the results.txt file
  currency citext,
  price citext
);

CREATE TABLE canonical_mfr(
  listing_mfr citext primary key,
  product_mfr citext
);

-- Load products
copy json(doc) from :products_txt csv quote e'\x01' delimiter e'\x02';
INSERT INTO products(name, mfr, model, family) SELECT doc->>'product_name', doc->>'manufacturer', doc->>'model', doc->>'family' FROM json;
UPDATE products SET clean_product = lower(trim(rpad(regexp_replace(model, '\yPEN\y-?|D[MS]C-?|Tough | ', '', 'g'), 10)));
--    trim(rpad(coalesce(family || ' ', '') || model, 30));

CREATE INDEX ON products(mfr);

-- Load listings
DELETE FROM json;
copy json(doc) from :listings_txt csv quote e'\x01' delimiter e'\x02';
INSERT INTO listings(title, mfr, currency, price) SELECT doc->>'title', doc->>'manufacturer', doc->>'currency', doc->>'price'  FROM json;

-- Tidy up listings:
--   1) turn dashes into spaces
--   2) remove family and mfr names
--   3) strip out low-value stuff after commas, plus signs

UPDATE listings SET clean_listing = 
  trim(rpad(
      regexp_replace(
        regexp_replace(
          regexp_replace(
            regexp_replace(lower(unaccent(title)), '[-_]', ' ', 'g'),
            '\(.*|\+.*|[0-9][0-9.,]+ ?mp.*|[0-9][0-9,.]+ ?mio.*|[0-9][0-9,.]+ mega.*|[0-9][0-9,.]+ mpix.*|/.*|with.*|,.*|\yfor\y.*|\yfur\y.*|\ypour\y.*|\yfür\y.*',
            ''), -- e.g. Nikon D90, Nikon's newest camera!
          '\yagfa\y|\ycanon\y|\ycasio\y|\yepson\y|\yfuji\y|\yfuji?film\y|\yhp\y|\ykodak\y|\ykonica\y|\yminolta\y|\ykyocera\y|' ||
          '\yleica\y|\ynikon\y|\yolympus\y|\ypanasonic\y|\ypentax\y|\ypolaroid\y|\yricoh\y|\ysamsung\y|\ysony\y|\ytoshiba\y|' ||
          '\yephoto\y|\yhs\y|\yelph\y|\yeos\y|\yrebel\y|\yixus\y|\yixy\y|\ykiss\y|\ypowershot\y|\ypower shot\y|' ||
          '\yis\y|\ydigital\y|\yexilim\y|\yphotopc\y|\yexr\y|\yfinepix\y|\yzoom\y|\yphotosmart\y|\yeasyshare\y|' ||
          '\yplus\y|\ycamera\y|\ysport\y|\yalpha\y|\yfinecam\y|\ycoolpix\y|\ycool pix\y|\ystylus\y|\ymju\y|' ||
          '\ylumix\y|\ydmc\y|\ydsc\y|\yoptio\y|\ycaplio\y|\ycybershot\y|\ycyber shot\y|\ymavica\y|\ydigitalkamera\y|' ||
          '\ypen\y|\ytough\y|\yred\y|\yblack\y|\yblue\y|\ywhite\y|\ysilver\y|\ypink\y|\ycompact\y|\ystarry\y|\ystill\y|' ||
          '\ycmos\y|\yappareil\y|\yd?slr\y|\yd?slr|d?slr\y|\yphoto\y|\ykamera\y|\ynume?rique\y|\yreflex\y.*|\ysystemkamera\y|' ||
          '\ybody\y|\ypure white\y|\ychampagne\y|\yblanc\y|\yplatinum\y|\ynoir\y|\yrouge\y|\yschwarz\y|\yweis\y|\yweiss\y|' ||
          '\ygreen\y|\ykit\y',
          ' ',
          'g'),
          ' ',
          '',
          'g')
        , 10));

-- Create a mapping from listing mfrs to product mfrs
WITH
  listing_mfrs AS (SELECT DISTINCT mfr, trim(rpad(mfr, 10))::citext as short FROM listings),
  product_mfrs AS (SELECT DISTINCT mfr, trim(rpad(mfr, 10))::citext as short FROM products)
INSERT INTO canonical_mfr(listing_mfr, product_mfr)
SELECT
  l.mfr,
  (SELECT mfr FROM product_mfrs p WHERE p.short <-> l.short <= 0.75 ORDER BY p.short <-> l.short LIMIT 1)
FROM listing_mfrs l;

CREATE INDEX ON products(lower(mfr));

-- Prune listings where no mfr exists in the set of known products
DELETE FROM listings WHERE EXISTS(SELECT * FROM canonical_mfr WHERE listing_mfr = mfr AND product_mfr IS NULL);


CREATE TABLE results AS
SELECT
  l.*,
  (SELECT p.id FROM products p
    JOIN canonical_mfr cm ON cm.product_mfr = p.mfr
    WHERE cm.listing_mfr = l.mfr AND clean_listing <-> clean_product < 0.5
    ORDER BY clean_listing <-> clean_product
    LIMIT 1) AS product_id,
  (SELECT 
    clean_listing <-> clean_product
    FROM products p
    JOIN canonical_mfr cm ON cm.product_mfr = p.mfr
    WHERE cm.listing_mfr = l.mfr
    ORDER BY clean_listing <-> clean_product
    LIMIT 1) AS score

  FROM listings l;

WITH unaggregated_results AS (
  SELECT products.name AS product_name,
    results.title,
    results.currency,
    results.price
  FROM results
  JOIN products ON product_id = products.id
), aggregated_results AS (
  SELECT product_name, array_agg(unaggregated_results) AS listings
  FROM unaggregated_results
  GROUP BY product_name
)
SELECT row_to_json(aggregated_results) FROM aggregated_results;
