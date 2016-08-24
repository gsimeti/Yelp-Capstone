library(jsonlite)
library(readr)
library(dplyr)
library(ggplot2)
library(tidyjson)
install.packages("data.table")
library(data.table)
library(plyr)
install.packages("ggmap")
library(ggmap)
install.packages("rpart")
library(rpart)
install.packages("rpart.plot")
library(rpart.plot)
install.packages("zoo")
library(zoo)
library(caTools)
install.packages("randomForest")

# review <- stream_in(file("yelp_academic_dataset_review.json"), pagesize = 1000)
# new_review <- subset(review, select = c(user_id, review_id, stars, date, type, 
#                                       business_id))
# write_csv(new_review, "new_review.csv")

# install.packages("tidyjson")


new_review <- read_csv("new_review.csv")
new_review$type <- NULL
str(new_review)

business <- stream_in(file("yelp_academic_dataset_business.json"), pagesize = 1000)
str(business)

categories <- business$categories
str(categories)

framed <- function(data) {
    nCol <- max(vapply(data, length, 0))
    data <- lapply(data, function(row) c(row, rep(NA, nCol - length(row))))
    data <- matrix(unlist(data), nrow = length(data), ncol = nCol, byrow = TRUE)
    data.frame(data, stringsAsFactors = FALSE)
}

categories1 <- framed(categories)
str(categories1)
colnames(categories1) <- c("Categories", "Categories1", "Categories2", "Categories3", 
                           "Categories4", "Categories5", "Categories6", "Categories7",
                           "Categories8", "Categories9")

hood <- business$neighborhoods
hood[hood == "character(0)"] <- NA
hood <- as.character(hood)
business$neighborhoods <- hood

business$categories <- NULL
business <- flatten(business)
new_business <- cbind(business, categories1)
str(new_business)

user <- stream_in(file("yelp_academic_dataset_user.json"), pagesize = 1000)
user$friends <- NULL
user$compliments <- NULL
user$votes <- NULL

elites <- user$elite
elites <- framed(elites)
elites[is.na(elites) == 1] <- 0
elite_tf <- vector(mode = "logical", length = 552339)
elite_tf <- ifelse(rowSums(elites) == 0, F, T)

user$elite <- NULL
user$name <- NULL
user$fans <- NULL
user$yelping_since <- NULL
user$type <- NULL
user <- cbind(user, elite_tf)

remove <- c(6, 12:26, 33, 42:48, 53:56, 72:90)
business_df <- new_business[-remove]
business_df$neighborhoods[business_df$neighborhoods == "NA"] <- NA

names(new_review)[names(new_review) == "stars"] <- "review_stars"
names(business_df)[names(business_df) == "stars"] <- "avg_biz_stars"
names(user)[names(user) == "average_stars"] <- "avg_user_stars"
names(business_df)[names(business_df) == "review_count"] <- "business_reviews_count"
names(user)[names(user) == "review_count"] <- "user_reviews_count"
names(user)[names(user) == "elite_tf"] <- "user_elite"

rev_us <- inner_join(new_review, user)
rev_us_biz <- inner_join(rev_us, business_df)

rev_us_biz %>% group_by(state) %>%
    summarise(cnt = n()) %>%
    ungroup() %>%
    arrange(desc(cnt))
## NV = 958641
## AZ = 848387
## Use AZ

az_biz <- subset(rev_us_biz, state == "AZ")
write_csv(az_biz, "az_biz.csv")
az_biz <- read_csv("az_biz.csv")
restos <- subset(business_df, state == "AZ")
restaurants <- subset(restos, Categories == "Restaurants" | Categories1 == "Restaurants" |
                          Categories2 == "Restaurants" | Categories3 == "Restaurants")
write_csv(restaurants, "az_restaurants.csv")
restaurants <- read_csv("az_restaurants.csv")

rm <- c(34, 62:67)
az_biz <- az_biz[ -rm]
write_csv(az_biz, "az_biz.csv")

az_rest <- subset(az_biz, Categories == "Restaurants" | Categories1 == "Restaurants" |
                      Categories2 == "Restaurants" | Categories3 == "Restaurants")
n_distinct(az_rest$business_id)

# Restaurants DF rename
names(restaurants)[names(restaurants) == "attributes.Take-out"] <- "take_out"
names(restaurants)[names(restaurants) == "attributes.Drive-Thru"] <- "drive_thru"
names(restaurants)[names(restaurants) == "attributes.Caters"] <- "caters"
names(restaurants)[names(restaurants) == "attributes.Noise Level"] <- "noise_level"
names(restaurants)[names(restaurants) == "attributes.Takes Reservations"] <- "reservations"
names(restaurants)[names(restaurants) == "attributes.Delivery"] <- "delivery"
names(restaurants)[names(restaurants) == "attributes.Outdoor Seating"] <- "outdoor"
names(restaurants)[names(restaurants) == "attributes.Attire"] <- "attire"
names(restaurants)[names(restaurants) == "attributes.Alcohol"] <- "alcohol"
names(restaurants)[names(restaurants) == "attributes.Waiter Service"] <- "waiter"
names(restaurants)[names(restaurants) == "attributes.Accepts Credit Cards"] <- "ccs"
names(restaurants)[names(restaurants) == "attributes.Good for Kids"] <- "kids"
names(restaurants)[names(restaurants) == "attributes.Good For Groups"] <- "groups"
names(restaurants)[names(restaurants) == "attributes.Price Range"] <- "price_range"
names(restaurants)[names(restaurants) == "attributes.BYOB"] <- "byob"
names(restaurants)[names(restaurants) == "attributes.Corkage"] <- "corkage"
names(restaurants)[names(restaurants) == "attributes.Order at Counter"] <- "counter"
names(restaurants)[names(restaurants) == "attributes.Good For.dessert"] <- "dessert"
names(restaurants)[names(restaurants) == "attributes.Good For.latenight"] <- "late_night"
names(restaurants)[names(restaurants) == "attributes.Good For.lunch"] <- "lunch"
names(restaurants)[names(restaurants) == "attributes.Good For.dinner"] <- "dinner"
names(restaurants)[names(restaurants) == "attributes.Good For.brunch"] <- "brunch"
names(restaurants)[names(restaurants) == "attributes.Good For.breakfast"] <- "breakfast"
names(restaurants)[names(restaurants) == "attributes.Ambience.romantic"] <- "romantic"
names(restaurants)[names(restaurants) == "attributes.Ambience.intimate"] <- "intimate"
names(restaurants)[names(restaurants) == "attributes.Ambience.classy"] <- "classy"
names(restaurants)[names(restaurants) == "attributes.Ambience.hipster"] <- "hipster"
names(restaurants)[names(restaurants) == "attributes.Ambience.divey"] <- "divey"
names(restaurants)[names(restaurants) == "attributes.Ambience.touristy"] <- "touristy"
names(restaurants)[names(restaurants) == "attributes.Ambience.trendy"] <- "trendy"
names(restaurants)[names(restaurants) == "attributes.Ambience.upscale"] <- "upscale"
names(restaurants)[names(restaurants) == "attributes.Ambience.casual"] <- "casual"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.dairy-free"] <- "dairy_free"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.gluten-free"] <- "gluten_free"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.vegan"] <- "vegan"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.kosher"] <- "kosher"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.halal"] <- "halal"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.soy-free"] <- "soy_free"
names(restaurants)[names(restaurants) == "attributes.Dietary Restrictions.vegetarian"] <- "vegetarian"


names(az_rest)[names(az_rest) == "attributes.Take-out"] <- "take_out"
names(az_rest)[names(az_rest) == "attributes.Drive-Thru"] <- "drive_thru"
names(az_rest)[names(az_rest) == "attributes.Caters"] <- "caters"
names(az_rest)[names(az_rest) == "attributes.Noise Level"] <- "noise_level"
names(az_rest)[names(az_rest) == "attributes.Takes Reservations"] <- "reservations"
names(az_rest)[names(az_rest) == "attributes.Delivery"] <- "delivery"
names(az_rest)[names(az_rest) == "attributes.Outdoor Seating"] <- "outdoor"
names(az_rest)[names(az_rest) == "attributes.Attire"] <- "attire"
names(az_rest)[names(az_rest) == "attributes.Alcohol"] <- "alcohol"
names(az_rest)[names(az_rest) == "attributes.Waiter Service"] <- "waiter"
names(az_rest)[names(az_rest) == "attributes.Accepts Credit Cards"] <- "ccs"
names(az_rest)[names(az_rest) == "attributes.Good for Kids"] <- "kids"
names(az_rest)[names(az_rest) == "attributes.Good For Groups"] <- "groups"
names(az_rest)[names(az_rest) == "attributes.Price Range"] <- "price_range"
names(az_rest)[names(az_rest) == "attributes.BYOB"] <- "byob"
names(az_rest)[names(az_rest) == "attributes.Corkage"] <- "corkage"
names(az_rest)[names(az_rest) == "attributes.Order at Counter"] <- "counter"
names(az_rest)[names(az_rest) == "attributes.Good For.dessert"] <- "dessert"
names(az_rest)[names(az_rest) == "attributes.Good For.latenight"] <- "late_night"
names(az_rest)[names(az_rest) == "attributes.Good For.lunch"] <- "lunch"
names(az_rest)[names(az_rest) == "attributes.Good For.dinner"] <- "dinner"
names(az_rest)[names(az_rest) == "attributes.Good For.brunch"] <- "brunch"
names(az_rest)[names(az_rest) == "attributes.Good For.breakfast"] <- "breakfast"
names(az_rest)[names(az_rest) == "attributes.Ambience.romantic"] <- "romantic"
names(az_rest)[names(az_rest) == "attributes.Ambience.intimate"] <- "intimate"
names(az_rest)[names(az_rest) == "attributes.Ambience.classy"] <- "classy"
names(az_rest)[names(az_rest) == "attributes.Ambience.hipster"] <- "hipster"
names(az_rest)[names(az_rest) == "attributes.Ambience.divey"] <- "divey"
names(az_rest)[names(az_rest) == "attributes.Ambience.touristy"] <- "touristy"
names(az_rest)[names(az_rest) == "attributes.Ambience.trendy"] <- "trendy"
names(az_rest)[names(az_rest) == "attributes.Ambience.upscale"] <- "upscale"
names(az_rest)[names(az_rest) == "attributes.Ambience.casual"] <- "casual"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.dairy-free"] <- "dairy_free"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.gluten-free"] <- "gluten_free"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.vegan"] <- "vegan"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.kosher"] <- "kosher"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.halal"] <- "halal"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.soy-free"] <- "soy_free"
names(az_rest)[names(az_rest) == "attributes.Dietary Restrictions.vegetarian"] <- "vegetarian"

summary(az_rest)

az_rest$dairy_free <- NULL
az_rest$gluten_free <- NULL
az_rest$vegan <- NULL
az_rest$kosher <- NULL
az_rest$halal <- NULL
az_rest$soy_free <- NULL
az_rest$vegetarian <- NULL

cuisine <- vector(mode = "character", length = 427692)
az_rest <- cbind(az_rest, cuisine)
az_rest$cuisine <- ifelse(az_rest$Categories == "Mexican" | az_rest$Categories1 == "Mexican" |
                      az_rest$Categories2 == "Mexican" | az_rest$Categories3 == "Mexican",
                  "Mexican", NA)
az_rest$cuisine[az_rest$Categories == "American (Traditional)" | az_rest$Categories1 == 
                    "American (Traditional)" | az_rest$Categories2 == "American (Traditional)" | 
                    az_rest$Categories3 == "American (Traditional)"] <- "American"
az_rest$cuisine[az_rest$Categories == "American (New)" | az_rest$Categories1 == 
                    "American (New)" | az_rest$Categories2 == "American (New)" | 
                    az_rest$Categories3 == "American (New)"] <- "American"
az_rest$cuisine[az_rest$Categories == "Barbeque" | az_rest$Categories1 == 
                    "Barbeque" | az_rest$Categories2 == "Barbeque" | 
                    az_rest$Categories3 == "Barbeque"] <- "American"
az_rest$cuisine[az_rest$Categories == "Burgers" | az_rest$Categories1 == 
                    "Burgers" | az_rest$Categories2 == "Burgers" | 
                    az_rest$Categories3 == "Burgers"] <- "American"
az_rest$cuisine[az_rest$Categories == "Steakhouses" | az_rest$Categories1 == 
                    "Steakhouses" | az_rest$Categories2 == "Steakhouses" | 
                    az_rest$Categories3 == "Steakhouses"] <- "American"
az_rest$cuisine[az_rest$Categories == "Southern" | az_rest$Categories1 == 
                    "Southern" | az_rest$Categories2 == "Southern" | 
                    az_rest$Categories3 == "Southern"] <- "American"
az_rest$cuisine[az_rest$Categories == "Soul Food" | az_rest$Categories1 == 
                    "Soul Food" | az_rest$Categories2 == "Soul Food" | 
                    az_rest$Categories3 == "Soul Food"] <- "American"
az_rest$cuisine[az_rest$Categories == "Italian" | az_rest$Categories1 == 
                               "Italian" | az_rest$Categories2 == "Italian" | 
                               az_rest$Categories3 == "Italian"] <- "Italian"
az_rest$cuisine[az_rest$Categories == "Japanese" | az_rest$Categories1 == 
                    "Japanese" | az_rest$Categories2 == "Japanese" | 
                    az_rest$Categories3 == "Japanese"] <- "Japanese"
az_rest$cuisine[az_rest$Categories == "Sushi" | az_rest$Categories1 == 
                    "Sushi" | az_rest$Categories2 == "Sushi" | 
                    az_rest$Categories3 == "Sushi"] <- "Japanese"
az_rest$cuisine[az_rest$Categories == "Sushi Bars" | az_rest$Categories1 == 
                    "Sushi Bars" | az_rest$Categories2 == "Sushi Bars" | 
                    az_rest$Categories3 == "Sushi Bars"] <- "Japanese"
az_rest$cuisine[az_rest$Categories == "Ramen" | az_rest$Categories1 == 
                    "Ramen" | az_rest$Categories2 == "Ramen" | 
                    az_rest$Categories3 == "Ramen"] <- "Japanese"
az_rest$cuisine[az_rest$Categories == "Chinese" | az_rest$Categories1 == 
                    "Chinese" | az_rest$Categories2 == "Chinese" | 
                    az_rest$Categories3 == "Chinese"] <- "Chinese"
az_rest$cuisine[az_rest$Categories == "Cantonese" | az_rest$Categories1 == 
                    "Cantonese" | az_rest$Categories2 == "Cantonese" | 
                    az_rest$Categories3 == "Cantonese"] <- "Chinese"
az_rest$cuisine[az_rest$Categories == "Szechuan" | az_rest$Categories1 == 
                    "Szechuan" | az_rest$Categories2 == "Szechuan" | 
                    az_rest$Categories3 == "Szechuan"] <- "Chinese"
az_rest$cuisine[az_rest$Categories == "Dim Sum" | az_rest$Categories1 == 
                    "Dim Sum" | az_rest$Categories2 == "Dim Sum" | 
                    az_rest$Categories3 == "Dim Sum"] <- "Chinese"
az_rest$cuisine[az_rest$Categories == "Shanghainese" | az_rest$Categories1 == 
                    "Shanghainese" | az_rest$Categories2 == "Shanghainese" | 
                    az_rest$Categories3 == "Shanghainese"] <- "Chinese"
az_rest$cuisine[az_rest$Categories == "Taiwanese" | az_rest$Categories1 == 
                    "Taiwanese" | az_rest$Categories2 == "Taiwanese" | 
                    az_rest$Categories3 == "Taiwanese"] <- "Chinese"
az_rest$cuisine[az_rest$Categories == "Thai" | az_rest$Categories1 == 
                    "Thai" | az_rest$Categories2 == "Thai" | 
                    az_rest$Categories3 == "Thai"] <- "Thai"
az_rest$cuisine[az_rest$Categories == "Greek" | az_rest$Categories1 == 
                    "Greek" | az_rest$Categories2 == "Greek" | 
                    az_rest$Categories3 == "Greek"] <- "Greek"
az_rest$cuisine[az_rest$Categories == "Vietnamese" | az_rest$Categories1 == 
                    "Vietnamese" | az_rest$Categories2 == "Vietnamese" | 
                    az_rest$Categories3 == "Vietnamese"] <- "Vietnamese"
az_rest$cuisine[az_rest$Categories == "Indian" | az_rest$Categories1 == 
                    "Indian" | az_rest$Categories2 == "Indian" | 
                    az_rest$Categories3 == "Indian"] <- "Indian"
az_rest$cuisine[az_rest$Categories == "Korean" | az_rest$Categories1 == 
                    "Korean" | az_rest$Categories2 == "Korean" | 
                    az_rest$Categories3 == "Korean"] <- "Korean"
az_rest$cuisine[az_rest$Categories == "Mediterranean" | az_rest$Categories1 == 
                    "Mediterranean" | az_rest$Categories2 == "Mediterranean" | 
                    az_rest$Categories3 == "Mediterranean"] <- "Mediterranean"
az_rest$cuisine[az_rest$Categories == "Turkish" | az_rest$Categories1 == 
                    "Turkish" | az_rest$Categories2 == "Turkish" | 
                    az_rest$Categories3 == "Turkish"] <- "Mediterranean"
az_rest$cuisine[az_rest$Categories == "Lebanese" | az_rest$Categories1 == 
                    "Lebanese" | az_rest$Categories2 == "Lebanese" | 
                    az_rest$Categories3 == "Lebanese"] <- "Mediterranean"
az_rest$cuisine[az_rest$Categories == "Moroccan" | az_rest$Categories1 == 
                    "Moroccan" | az_rest$Categories2 == "Moroccan" | 
                    az_rest$Categories3 == "Moroccan"] <- "Mediterranean"
az_rest$cuisine[az_rest$Categories == "Middle Eastern" | az_rest$Categories1 == 
                    "Middle Eastern" | az_rest$Categories2 == "Middle Eastern" | 
                    az_rest$Categories3 == "Middle Eastern"] <- "Middle Eastern"
az_rest$cuisine[az_rest$Categories == "Persian/Iranian" | az_rest$Categories1 == 
                    "Persian/Iranian" | az_rest$Categories2 == "Persian/Iranian" | 
                    az_rest$Categories3 == "Persian/Iranian"] <- "Middle Eastern"
az_rest$cuisine[az_rest$Categories == "Pakistani" | az_rest$Categories1 == 
                    "Pakistani" | az_rest$Categories2 == "Pakistani" | 
                    az_rest$Categories3 == "Pakistani"] <- "Middle Eastern"
az_rest$cuisine[az_rest$Categories == "Afghan" | az_rest$Categories1 == 
                    "Afghan" | az_rest$Categories2 == "Afghan" | 
                    az_rest$Categories3 == "Afghan"] <- "Middle Eastern"
az_rest$cuisine[az_rest$Categories == "Arabian" | az_rest$Categories1 == 
                    "Arabian" | az_rest$Categories2 == "Arabian" | 
                    az_rest$Categories3 == "Arabian"] <- "Middle Eastern"
az_rest$cuisine[az_rest$Categories == "French" | az_rest$Categories1 == 
                    "French" | az_rest$Categories2 == "French" | 
                    az_rest$Categories3 == "French"] <- "French"
az_rest$cuisine[az_rest$Categories == "Creperies" | az_rest$Categories1 == 
                    "Creperies" | az_rest$Categories2 == "Creperies" | 
                    az_rest$Categories3 == "Creperies"] <- "French"
az_rest$cuisine[az_rest$Categories == "Spanish" | az_rest$Categories1 == 
                    "Spanish" | az_rest$Categories2 == "Spanish" | 
                    az_rest$Categories3 == "Spanish"] <- "Spanish"
az_rest$cuisine[az_rest$Categories == "Tapas Bars" | az_rest$Categories1 == 
                    "Tapas Bars" | az_rest$Categories2 == "Tapas Bars" | 
                    az_rest$Categories3 == "Tapas Bars"] <- "Spanish"
az_rest$cuisine[az_rest$Categories == "Basque" | az_rest$Categories1 == 
                    "Basque" | az_rest$Categories2 == "Basque" | 
                    az_rest$Categories3 == "Basque"] <- "Spanish"
az_rest$cuisine <- ifelse(is.na(az_rest$cuisine), 1, az_rest$cuisine)
az_rest$cuisine <- ifelse(az_rest$cuisine == 1, "Other", az_rest$cuisine)

az_rest$neighborhoods <- NULL

f <- c(20, 24, 25, 53)
az_rest[,f] <- lapply(az_rest[,f], factor)

write_csv(az_rest, "az_rest.csv")
write_csv(restaurants, "restaurants_df.csv")

az_rest <- read_csv("az_rest.csv")
restaurants <- read_csv("restaurants_df.csv")
library(randomForest)

# Moving Average by Cuisine
rating_date <- ggplot(az_rest, aes(date, review_stars)) + geom_jitter(aes(color = cuisine),
                                                                      alpha = 0.3) +
    geom_smooth()
rating_date + facet_wrap(~cuisine, nrow = 4)

# Rating by Price Range

pop_rating <- ggplot(restaurants, aes(business_reviews_count, avg_biz_stars)) +
    geom_point(aes(size = price_range))
pop_rating

rest_sub <- subset(restaurants, restaurants$avg_biz_stars >= 4)
mean(restaurants$avg_biz_stars)

r <- c(3, 6:8, 13, 16, 17, 19, 20, 23:30, 34:48, 53)
rest_reg <- az_rest[,r]
str(rest_reg)

# Imputation

rest_reg_imp <- rest_reg
rest_reg_imp$price_range <- as.factor(rest_reg_imp$price_range)
rest_reg_imp$review_stars <- as.factor(rest_reg_imp$review_stars)

rest_reg_imp$take_out[rest_reg_imp$take_out == NA] <- T
fact <- c(4, 7, 8, 10, 13:16, 18:32)
rest_reg_imp[,fact] <- lapply(rest_reg_imp[,fact], factor)
str(rest_reg_imp)

# Random Forest - Classification

rest_ran_for <- rest_reg_imp
set.seed(123)
rest_split <- sample.split(rest_ran_for$review_stars, SplitRatio = 0.7)
rest_train <- subset(rest_ran_for, rest_split == T)
rest_test <- subset(rest_ran_for, rest_split == F)
train_imp <- rfImpute(review_stars ~ ., rest_train)
model_rf <- randomForest(review_stars ~ ., data = rest_train, na.action = na.roughfix)
summary(model_rf)

library(caTools)
set.seed(123)
rest_split <- sample.split(rest_reg$review_stars, SplitRatio = 0.7)
rest_train <- subset(rest_reg, rest_split == T)
rest_test <- subset(rest_reg, rest_split == F)
lm_rest <- lm(review_stars ~ ., data = rest_train)
summary(lm_rest)
lin_reg_pred <- predict(lm_rest, newdata = rest_test)
lm.sse <- sum((lin_reg_pred - rest_test$review_stars)^2)
lm.sse

tree <- rpart(review_stars ~ ., data = rest_train)
prp(tree)

ggplot(rest_sub, aes(longitude, latitude)) + 
    geom_point(aes(color = avg_biz_stars)) +
    scale_color_gradient(limits = c(4.5, 5), low = "green", high = "black")

str(az_rest)
n_distinct(az_rest$user_id)
mean(az_rest$user_reviews_count)

az_rest %>% group_by(cuisine) %>%
    summarise(cnt = n()) %>%
    ungroup() %>%
    arrange(desc(cnt))