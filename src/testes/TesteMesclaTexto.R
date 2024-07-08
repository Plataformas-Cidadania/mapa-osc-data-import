
teste <- tribble(
  ~Grupo,   ~Texto, 
  2,        "b",
  1,        "f",
  1,        "o", 
  2,        "a", 
  1,        "o", 
  2,        "r", 
  2,        "r")

teste

teste %>% 
  group_by(Grupo) %>% 
  mutate(Mesclado = paste0(unique(Texto), collapse = ", "))


rm(teste)


