
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










