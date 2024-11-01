#load stringdist package
library(stringdist)

#calculate Levenshtein distance between two strings
stringdist("string1", "string2", method = "lv")

stringdist("joaoribeiro", "joaoribeirodasilva", method = "lv")

stringdist("joaoribeir", "boaorbairu", method = "lv")

stringdist("joaoribeiro", "jrsilva", method = "lv")

stringdist("umarizal", "umarival", method = "lv")


nchar("joaoribeir")
nchar("boaorbairo")