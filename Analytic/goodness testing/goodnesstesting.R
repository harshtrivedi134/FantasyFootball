actual = read.csv("week14.csv")
predicted = read.csv("goodnessinput.csv", header = F)
players = match(actual$Player, predicted[,1])
tomatch = actual[which(!is.na(players)),]
players = players[which(!is.na(players))]
for(i in 1:length(tomatch[,1])){
  tomatch$pred[i] = 0
}
for(i in 1:length(tomatch[,1])){
  tomatch$pred[i] = predicted[players[i],2]
}

fit = lm(tomatch$Fantasy.Points ~ tomatch$pred)
summary(fit)
plot(fit)


avgs = read.csv("playerlist.csv", header = F)
players = match(actual$Player, avgs[,2])
players = players[which(!is.na(players))]
names = avgs[players,2]
inall = match(names, tomatch$Player)
inall = which(!is.na(inall))
for(i in 1:length(tomatch[,1])){
  tomatch$avg[i] = 0
}
for(i in 1:length(tomatch[,1])){
  tomatch$avg[i] = avgs[inall[i],4]
}