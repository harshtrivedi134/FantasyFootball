library(rvest)

years<-2009:2016
weeks<-c(paste0("pre-season-week-", 1:4), paste0("week-", 1:17), "wildcard-weekend", "divisional-playoffs", "conf-championships", "pro-bowl", "superbowl")

load_weather<-function(year, week) {
  base_url<-"http://nflweather.com/week/"
  if (year == 2010) { # necessary because of different file naming
    start_url<-paste0(base_url, year, "/", week, "-2/")
  } else {
    start_url<-paste0(base_url, year, "/", week, "/")
  }
  if (year == 2013 && week == "pro-bowl") {
    return (NULL)
  }
  tryCatch ({  
    page<-html(start_url, encoding="ISO-8859-1") 
    table<-page %>% html_nodes("table")  %>% .[[1]] %>% html_table()
    table<-cbind("Year"=year, "Week"=week, table[,c("Away", "Home", "Forecast", "Extended Forecast", "Wind")])
    return(table)
  }, 
  
  error = function(e) { 
    print(paste(e, "Year", y, "Week", w))
    return(NULL)
  })
}

weather_data<-data.frame("Year"=integer(0), "Week"=character(0), "Away"=character(0), "Home"=character(0), "Forecast"=character(0), "Extended Forecast"=character(0), "Wind"=character(0))
for (y in years) {
  for (w in weeks) {
    weather_data<-rbind(weather_data, load_weather(y, w))
  }
}


write.csv(weather_data,file="Weather.csv")