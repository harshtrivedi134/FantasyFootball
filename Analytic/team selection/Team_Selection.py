import pandas as pd
#class describing how a player object would look like
class Player():
    def __init__(self, position, name, salary, points, value):
        self.self = self
        self.position = position
        self.name = name
        self.salary = salary
        self.points = points
        self.value = value

    def __iter__(self):
        return iter(self.list)

    def __str__(self):
        return "{} {} {} {}".format(self.name, self.position, self.salary, self.points)



def select_team(players):
    budget = 60000
    current_team_salary = 0
    # Contraints dict stating the max number of players allowed per position
    constraints = {
        'QB': 1,
        'RB': 2,
        'WR': 3,
        'TE':1,
        'K': 1,
        'DEF': 1
    }
    #Dict maintaining count of players per position
    counts = {
        'QB': 0,
        'RB': 0,
        'WR': 0,
        'TE': 0,
        'K': 0,
        'DEF': 0
    }

    # First we order the players by the value heuristic ( value = points/salary)

    players.sort(key=lambda x: x.value, reverse=True)

    #declare list to contin final team of player objects
    team = []

    for player in players:
        nam = player.name
        pos = player.position
        sal = player.salary
        pts = player.points
        #check position and salary constraint before adding each player
        if counts[pos] < constraints[pos] and current_team_salary + sal <= budget:
            team.append(player)
            counts[pos] = counts[pos] + 1
            current_team_salary += sal
            continue


            team.append(player)

            current_team_salary += sal

    #Now Iterate by ordering the players based on points and replae team members wherever necessry.

    players.sort(key=lambda x: x.points, reverse=True)
    for player in players:
        nam = player.name
        pos = player.position
        sal = player.salary
        pts = player.points
        if player not in team:
            pos_players = [x for x in team if x.position == pos]
            pos_players.sort(key=lambda x: x.points)
            for pos_player in pos_players:
                # if budget constraint holds after replacing the player, then go head with the replacement.
                if (current_team_salary + sal - pos_player.salary) <= budget and pts > pos_player.points:
                    team[team.index(pos_player)] = player
                    current_team_salary = current_team_salary + sal - pos_player.salary
                    break
    return team

players = []


df = pd.read_csv('playerlist.csv',encoding='latin-1')
for index,row in df.iterrows():
        name = row['name']
        position = row['position']
        salary = int(row['salary'])
        points = float(row['points'])
        value = points / salary
        player = Player(position, name, salary, points, value)
        players.append(player)

team = select_team(players)

salary = 0
for player in team:
    points += player.points
    salary += player.salary
    print(player)
print("\nPoints: {}".format(points))
print("Salary: {}".format(salary))




