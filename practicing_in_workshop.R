
# packages_and_data -------------------------------------------------------

library(arrow)
library(dplyr)
library(dbplyr)
library(duckdb)
library(stringr)
library(lubridate)
library(palmerpenguins)
library(tictoc)
library(scales)
library(janitor)
library(fs)

# extras (optional)
library(ggplot2)
library(ggrepel)
library(sf)


# dataset -----------------------------------------------------------------

nyc_taxi <- open_dataset(here::here("data/nyc-taxi-tiny/"))

# list.files(here::here("data/nyc-taxi-tiny/year=2018"))

# taxi zone lookup file
zones <- read_csv_arrow("data/taxi_zone_lookup.csv")
zones


# shapefile
shapefile <- "data/taxi_zones/taxi_zones.shp"
shapedata <- sf::read_sf(shapefile)

# checking on data
nyc_taxi
nrow(nyc_taxi)

# dplyr pipeline

zone_counts <- nyc_taxi %>%
	count(dropoff_location_id) %>%
	arrange(desc(n)) %>%
	collect()

# timing code
tic()
nyc_taxi %>%
	count(dropoff_location_id) %>%
	arrange(desc(n)) %>%
	collect() %>%
	invisible()
toc()

# extras

left_join(
	x = shapedata,
	y = zone_counts,
	by = c("LocationID" = "dropoff_location_id")
) |>
	ggplot(aes(fill = n)) +
	geom_sf(size = .1) +
	scale_fill_distiller(
		name = "Number of trips",
		limits = c(0, 1700000),
		labels = label_comma(),
		direction = 1
	) +
	theme_bw() +
	theme(panel.grid = element_blank())



# Hello Arrow -------------------------------------------------------------

nyc_taxi %>%
	head() %>%
	collect()

nyc_taxi %>%
	filter(year %in% 2017:2021) %>%
	group_by(year) %>%
	summarise(
		all_trips = n(),
		shared_trips = sum(passenger_count > 1, na.rm = TRUE)
	) %>%
	mutate(pct_shared = shared_trips / all_trips * 100) %>%
	collect()

# Exercise
# 1.Calculate the total number of rides for every month in 2019 ?
# 2.For each month in 2019, find the distance travelled by the
#   longest recorded taxi ride that month and sort the results
#   in month order ?

# 1
nyc_taxi %>%
	filter(year == 2019) %>%
	count(month) %>%
	collect()

# 2
nyc_taxi %>%
	filter(year == 2019) %>%
	group_by(month) %>%
	summarise(
		longest_distance = max(trip_distance, na.rm = TRUE)
	) %>%
	arrange(month) %>%
	collect()



# Part 2: data wrangling with Arrow ---------------------------------------


nyc_taxi <- open_dataset("data/nyc-taxi-tiny/")

shared_rides <- nyc_taxi %>%
  filter(year %in% 2017:2021) %>%
  group_by(year) %>%
  summarise(
    all_trips = n(),
    shared_trips = sum(passenger_count > 1, na.rm = TRUE)
  ) %>%
  mutate(
    pct_shared = shared_trips / all_trips * 100
  )

tic()
collect(shared_rides)
toc()

## limitations ----

millions <- function(x) x / 10^6

shared_rides %>%
  mutate(
    all_trips = millions(all_trips),
    shared_trips = millions(shared_trips)
  ) %>%
  collect()

# but scoped verbs like `mutate_at` or `across` can't be translated to Arrow
# c++ right now. So using these before `collect` fails.

# shared_rides %>%
#   mutate_at(c("all_trips", "shared_trips"), millions) %>%
#   collect()
#
# shared_rides %>%
#   mutate(
#     across(ends_with("trips"), millions)
#   ) %>%
#   collect()

# But we can use these scoped verbs once we do collect a small table into R

shared_rides %>%
  collect() %>%
  mutate_at(c("all_trips", "shared_trips"), millions)

shared_rides %>%
	collect() %>%
	mutate(
		across(ends_with("trips"), millions)
	)


## Moving csv to Arrow table -----

nyc_taxi_zones <- read_csv_arrow("data/taxi_zone_lookup.csv") %>%
  clean_names()

# moving this tibble into Arrow

nyc_taxi_zones_arrow <- arrow_table(nyc_taxi_zones)


## String manipulation -----

# If we would do this in R

nyc_taxi_zones %>%
  mutate(
    abbr_zone = zone %>%
      str_remove_all("[aeiou' ]") %>%
      str_remove_all("/.*"),
    abbr_zone_length = str_length(abbr_zone)
  ) %>%
  select(zone, abbr_zone, abbr_zone_length) %>%
  arrange(desc(abbr_zone_length))

# But with Arrow

nyc_taxi_zones_arrow %>%
  mutate(
    abbr_zone = zone %>%
      str_replace_all("[aeiou' ]", "") %>%
      str_replace_all("/.*", ""),
    abbr_zone_length = str_length(abbr_zone)
  ) %>%
  select(zone, abbr_zone, abbr_zone_length) %>%
  arrange(desc(abbr_zone_length)) %>%
  collect()

# str_remove_all is not supported in Arrow


## Reading csv data directly into Arrow memory

read_csv_arrow("data/taxi_zone_lookup.csv", as_data_frame = FALSE) %>%
  rename(
    location_id = LocationID,
    borough = Borough,
    zone = Zone
  ) %>%
  mutate(
    abbr_zone = zone %>%
      str_replace_all("[aeiou' ]", "") %>%
      str_replace_all("/.*", ""),
    abbr_zone_length = str_length(abbr_zone)
  ) %>%
  select(zone, abbr_zone, abbr_zone_length) %>%
  arrange(desc(abbr_zone_length)) %>%
  collect()


## working with date times ------

nyc_taxi %>%
  filter(
    year == 2022,
    month == 1
  ) %>%
  mutate(
    day = day(pickup_datetime),
    weekday = wday(pickup_datetime, label = TRUE),
    hour = hour(pickup_datetime),
    minute = minute(pickup_datetime),
    second = second(pickup_datetime)
  ) %>%
  filter(
    hour == 3,
    # minute == 14,
    # second == 15
  ) %>%
  select(
    pickup_datetime, year, month, day, weekday, hour
  ) %>%
  collect()


## Two table computations -----

location <- arrow_table(
  island = c("Trogersen", "Biscoe", "Dream"),
  lon = c(-64.11, -65.43, -64.73),
  lat = c(-64.08, -65.50, -64.23)
)

penguins %>%
  arrow_table() %>%
  left_join(location) %>%
  select(species, island, bill_length_mm, lon, lat) %>%
  collect()


pickup <- nyc_taxi_zones %>%
  select(
    pickup_location_id = location_id,
    pickup_borough = borough
  )

nyc_taxi %>%
  left_join(pickup) %>%
  collect()


nyc_taxi$schema
arrow_table(pickup)$schema

# now pickup_location_id variable is not of same type in both nyc_taxi and
# nyc_taxi_zones arrow data

# to set it same

nyc_taxi_zones <- nyc_taxi_zones %>%
  as_arrow_table(
    schema = schema(
      location_id = int64(),
      borough = utf8(),
      zone = utf8(),
      service_zone = utf8()
    )
  )


pickup <- nyc_taxi_zones %>%
  select(
    pickup_location_id = location_id,
    pickup_borough = borough
  )

dropoff <- nyc_taxi_zones %>%
  select(
    dropoff_location_id = location_id,
    dropoff_borough = borough
  )


tic()
borough_counts <- nyc_taxi %>%
  left_join(pickup) %>%
  left_join(dropoff) %>%
  count(pickup_borough, dropoff_borough) %>%
  arrange(desc(n)) %>%
  collect()
toc()

## Exercise----

nyc_taxi_zones %>%
  select(zone) %>%
  filter(str_detect(zone, "Airport")) %>%
  collect()

pickup_location <- nyc_taxi_zones %>%
  select(
    pickup_location_id = location_id,
    pickup_zone = zone
  )

nyc_taxi %>%
  filter(year == 2019) %>%
  left_join(pickup_location) %>%
  filter(str_detect(pickup_zone, "Airport")) %>%
  count(pickup_zone) %>%
  collect()


# duckdb and window funcition ---------------------------------------------

tic()
penguins %>%
  mutate(
    id = row_number()
  ) %>%
  filter(is.na(sex)) %>%
  select(id, sex, species, island)
toc()

# dplyr::row_number() is not supported in Arrow

# Engine supplied by Arrow c++ library doesn't support window function currently

# But duckdb engine does support window function

tic()
penguins %>%
  arrow_table() %>%
  to_duckdb() %>%
  mutate(id = row_number()) %>%
  filter(is.na(sex)) %>%
  select(id, sex, species, island)
toc()


tic()
numerology <- nyc_taxi %>%
  to_duckdb() %>%
  window_order(pickup_datetime) %>%
  mutate(trip_id = row_number()) %>%
  filter(
    trip_id %>% as.character() %>% str_detect("59"),
    second(pickup_datetime) == 59,
    minute(pickup_datetime) == 59
  ) %>%
  mutate(
    magic_number = trip_id %>%
      as.character() %>%
      str_remove_all("[^59]") %>%
      as.integer()
  ) %>%
  select(trip_id, magic_number, pickup_datetime) %>%
  collect()
toc()


# part 3: Data storage ----------------------------------------------------

parquet_file <- "data/nyc-taxi-tiny/year=2019/month=9/part-0.parquet"

nyc_taxi_2019_09 <- read_parquet(parquet_file)

class(nyc_taxi_2019_09) # tibble

parquet_file %>%
  read_parquet(col_select = matches("pickup"))


tic()
parquet_file %>%
  read_parquet() %>%
  invisible()
toc()

tic()
parquet_file %>%
  read_parquet(col_select = matches("pickup")) %>%
  invisible()
toc()


## exercise part3 ----

tic()
nyc_taxi_2019_09 %>% write_csv_arrow("data/nyc_taxi_2019_09.csv")
toc()

fs::file_size("data/nyc_taxi_2019_09.csv")

tic()
nyc_taxi_2019_09 %>% write_parquet("data/nyc_taxi_2019_09.parquet")
toc()

fs::file_size("data/nyc_taxi_2019_09.parquet")


tic()
read_csv_arrow("data/nyc_taxi_2019_09.csv")
toc() # 0.19 sec

tic()
read_csv_arrow("data/nyc_taxi_2019_09.csv", as_data_frame = FALSE)
toc() # 0.11 sec

tic()
read_parquet("data/nyc_taxi_2019_09.parquet")
toc() # 0.1 sec

tic()
read_parquet("data/nyc_taxi_2019_09.parquet", as_data_frame = FALSE)
toc() # 0.04 sec


## Multiile datasets -----

nyc_taxi <- open_dataset("data/nyc-taxi-tiny/")

tic()
nyc_taxi %>%
  filter(year == 2016, month == 9) %>%
  nrow()
toc()

tic()
nyc_taxi %>%
  filter(pickup_location_id == 138) %>%
  nrow()
toc()


## writing as parquet file with hive partitioning

nyc_taxi_2016a <- open_dataset(
  sources = "data/nyc-taxi-tiny/year=2016/",
  format = "parquet",
  unify_schemas = TRUE
)


tic()
nyc_taxi_2016a %>%
  group_by(month, vendor_name) %>%
  write_dataset("data/nyc-taxi_2016")
toc()

nyc_taxi_2016b <- open_dataset("data/nyc-taxi_2016/")
nyc_taxi_2016b


tic()
nyc_taxi_2016a %>%
  filter(vendor_name == "CMT") %>%
  group_by(month) %>%
  summarize(distance = sum(trip_distance, na.rm = TRUE)) %>%
  collect()
toc()

tic()
nyc_taxi_2016b %>%
  filter(vendor_name == "CMT") %>%
  group_by(month) %>%
  summarize(distance = sum(trip_distance, na.rm = TRUE)) %>%
  collect()
toc()


## Exercises part3 ------

# 1
nyc_taxi_2019a <- open_dataset("data/nyc-taxi-tiny/year=2019") %>%
  mutate(
    monthday = mday(pickup_datetime),
    yearday = yday(pickup_datetime)
  )

nyc_taxi_2019a %>%
  select(pickup_datetime, monthday, yearday) %>%
  collect()

# 2
nyc_taxi_2019a %>%
  group_by(month, monthday) %>%
  write_dataset("data/nyc-taxi_2019")

nyc_taxi_2019a %>%
  group_by(yearday) %>%
  write_dataset("data/nyc-taxi_2019_yearday")


# 3
nyc_taxi_2019_monthday <- open_dataset("data/nyc-taxi_2019/")

tic()
nyc_taxi_2019_monthday %>%
  filter(yearday %in% 81:90) %>%
  group_by(yearday) %>%
  summarise(
    money_charged = sum(total_amount, na.rm = TRUE)
  ) %>%
  collect()
toc()

nyc_taxi_2019_yearday <- open_dataset("data/nyc-taxi_2019_yearday/")

tic()
nyc_taxi_2019_yearday %>%
  filter(yearday %in% 81:90) %>%
  group_by(yearday) %>%
  summarise(
    money_charged = sum(total_amount, na.rm = TRUE)
  ) %>%
  collect()
toc()


# part 4: Advanced Arrow --------------------------------------------------

Scalar$create("new york")

Array$create(c("new york", NA, NA, "philadelphia"))

# R array is mutable but Arrow Array is immutable
# Arrow Array is comprised of a fixed set of buffers-contiguous blocks
# of memory dedicated to that array along with some metadata

riots <- arrow_table(
  location = chunked_array(
    c("Inn", "cafe", "do-nuts", "deweys"),
    c("Cross", "La Rambla")
  ),
  year = chunked_array(
    c(1969, 1966, 1959, 1965),
    c(1978, 1977)
  ),
  city = chunked_array(
    c("new york", "san francisco", "los angles", "philadelphia"),
    c("sydney", "barcelona")
  )
)

class(riots)

as.data.frame(riots) # returns tibble

# Arrow Record Batch is just a collection of Arrow arrays
# whereas, Arrow Table is a collection of Arrow chunked arrays


# Big Picture -------------------------------------------------------------


nyc_taxi <- open_dataset("data/nyc-taxi-tiny/")

nyc_taxi_zones <- read_csv_arrow(
  file = "data/taxi_zone_lookup.csv",
  as_data_frame = FALSE,
  skip = 1,
  schema = schema(
    LocationID = int64(),
    Borough = utf8(),
    Zone = utf8(),
    service_zone = utf8()
  )
) %>%
  rename(
    location_id = LocationID,
    borough = Borough,
    zone = Zone,
    service_zone = service_zone
  )

airport_zones <- nyc_taxi_zones %>%
  filter(str_detect(zone, "Airport")) %>%
  pull(location_id)

dropoff_zones <- nyc_taxi_zones %>%
  select(
    dropoff_location_id = location_id,
    dropoff_zone = zone
  )

airport_pickups <- nyc_taxi %>%
  filter(pickup_location_id %in% airport_zones) %>%
  select(
    matches("datetime"),
    matches("location_id")
  ) %>%
  left_join(dropoff_zones) %>%
  count(dropoff_zone) %>%
  arrange(desc(n)) %>%
  collect()


dat <- "data/taxi_zones/taxi_zones.shp" %>%
  read_sf() %>%
  clean_names() %>%
  left_join(airport_pickups,
            by = c("zone" = "dropoff_zone")) %>%
  arrange(desc(n))

windows()
dat %>%
  ggplot(aes(fill = n)) +
  geom_sf(size = 0.1, color = "#222222") +
  scale_fill_distiller(
    name = "Number of trips",
    labels = label_comma(),
    palette = "Oranges",
    direction = 1
  ) +
  geom_label_repel(
    stat = "sf_coordinates",
    data = dat %>%
      mutate(
        zone = case_when(
          str_detect(zone, "Airport") ~ zone,
          str_detect(zone, "Times") ~ zone,
          TRUE ~ ""
        )
      ),
    mapping = aes(label = zone, geometry = geometry),
    max.overlaps = 50,
    box.padding = 0.5,
    label.padding = 0.5,
    label.size = 0.15,
    label.r = 0,
    force = 30,
    force_pull = 0,
    fill = "white",
    min.segment.length = 0
  ) +
  theme_void() +
  theme(
    text = element_text(color = "black"),
    plot.background = element_rect(color = NA, fill = "#839496"),
    legend.background = element_rect(fill = "white"),
    legend.margin = margin(10, 10, 10, 10)
  )
