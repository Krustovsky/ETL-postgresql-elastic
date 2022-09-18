from elasticsearch import Elasticsearch, NotFoundError
from pathlib import Path
import json
from datetime import datetime

#ELS_PASSWORD = "eGxfFvZhtWmN+aDJGin1"
#BASE_DIR = Path(__file__).resolve().parent
# Create the client instance
#print(BASE_DIR / "http_ca.crt")
es = Elasticsearch(
    "http://127.0.0.1:9200",
    #ca_certs= BASE_DIR / "http_ca.crt",
    #verify_certs=False,
    #basic_auth=("elastic", ELS_PASSWORD),

)


with open(BASE_DIR / 'ELS_scheme.json', 'r') as f:
    request_body = json.load(f)
print(request_body)
#es.indices.delete(index='example_index')
#print(es.indices.create(index='example_index', **request_body))

try:
    indices_dict = es.indices.get(index='example_index')
except NotFoundError:
    es.indices.create(index='example_index', **request_body)
    indices_dict = es.indices.get(index='example_index')

print("\nindices_dict type:", type(indices_dict))

for index_name, value in indices_dict.items():
    print('index: ', value)

resp = es.bulk(index=index_name, operations =
    [
        {"index": {"_index": "example_index", "_id": "025c58cd-1b7e-43be-9ffb-8571a613579b"}},
        {
            "id": "6ecc7a32-14a1-4da8-9881-bf81f0f09897",
            "title": "Star Trek Into Darkness",
            "description": "When the USS Enterprise crew is called back home, they find an unstoppable force of terror from within their own organization has detonated the fleet and everything it stands for, leaving our world in a state of crisis. With a personal score to settle, Captain Kirk leads a manhunt to a war-zone world to capture a one-man weapon of mass destruction. As our space heroes are propelled into an epic chess game of life and death, love will be challenged, friendships will be torn apart, and sacrifices must be made for the only family Kirk has left: his crew.",
            "imdb_rating": 7.7,
            "actors": [{"id": "6960e2ca-889f-41f5-b728-1e7313e54d6c", "name": "Gene Roddenberry"},
                        {"id": "82b7dffe-6254-4598-b6ef-5be747193946", "name": "Alex Kurtzman"},
                        {"id": "9b58c99a-e5a3-4f24-8f67-a038665758d6", "name": "Roberto Orci"},
                        {"id": "dbac6947-e620-4f92-b6a1-dae9a3b07422", "name": "Damon Lindelof"}],
            "writers": [{"id": "6960e2ca-889f-41f5-b728-1e7313e54d6c", "name": "Gene Roddenberry"},
                        {"id": "82b7dffe-6254-4598-b6ef-5be747193946", "name": "Alex Kurtzman"},
                        {"id": "9b58c99a-e5a3-4f24-8f67-a038665758d6", "name": "Roberto Orci"},
                        {"id": "dbac6947-e620-4f92-b6a1-dae9a3b07422", "name": "Damon Lindelof"}],
            "genre": ["Action", "Adventure", "Sci-Fi"],
            "writers_names": ["Alex Kurtzman", "Damon Lindelof", "Gene Roddenberry", "Roberto Orci"],
            "actors_names": ["Chris Pine", "Karl Urban", "Zachary Quinto", "Zoe Saldana"],
            "director": ["J.J. Abrams"]
        }
    ]
)
print(resp)


resp = es.bulk(index=index_name, operations=
               [
                    {"index": {"_index": "example_index", "_id": "3d825f60-9fff-4dfe-b294-1a45fa1e115d"}},
                    {"id": "3d825f60-9fff-4dfe-b294-1a45fa1e115d", "imdb_rating": 8.6, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode IV - A New Hope", "description": "The Imperial Forces, under orders from cruel Darth Vader, hold Princess Leia hostage in their efforts to quell the rebellion against the Galactic Empire. Luke Skywalker and Han Solo, captain of the Millennium Falcon, work together with the companionable droid duo R2-D2 and C-3PO to rescue the beautiful princess, help the Rebel Alliance and restore freedom and justice to the Galaxy.", "director": ["George Lucas"], "actors_names": ["Carrie Fisher", "Harrison Ford", "Mark Hamill", "Peter Cushing"], "writers_names": ["George Lucas"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "0312ed51-8833-413f-bff5-0e139c11264a"}},
                    {"id": "0312ed51-8833-413f-bff5-0e139c11264a", "imdb_rating": 8.7, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode V - The Empire Strikes Back", "description": "Luke Skywalker, Han Solo, Princess Leia and Chewbacca face attack by the Imperial forces and its AT-AT walkers on the ice planet Hoth. While Han and Leia escape in the Millennium Falcon, Luke travels to Dagobah in search of Yoda. Only with the Jedi master's help will Luke survive when the dark side of the Force beckons him into the ultimate duel with Darth Vader.", "director": ["Irvin Kershner"], "actors_names": ["Billy Dee Williams", "Carrie Fisher", "Harrison Ford", "Mark Hamill"], "writers_names": ["George Lucas", "Lawrence Kasdan", "Leigh Brackett"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "025c58cd-1b7e-43be-9ffb-8571a613579b"}},
                    {"id": "025c58cd-1b7e-43be-9ffb-8571a613579b", "imdb_rating": 8.3, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode VI - Return of the Jedi", "description": "Luke Skywalker battles horrible Jabba the Hut and cruel Darth Vader to save his comrades in the Rebel Alliance and triumph over the Galactic Empire. Han Solo and Princess Leia reaffirm their love and team with Chewbacca, Lando Calrissian, the Ewoks and the androids C-3PO and R2-D2 to aid in the disruption of the Dark Side and the defeat of the evil emperor.", "director": ["Richard Marquand"], "actors_names": ["Billy Dee Williams", "Carrie Fisher", "Harrison Ford", "Mark Hamill"], "writers_names": ["George Lucas", "Lawrence Kasdan"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "cddf9b8f-27f9-4fe9-97cb-9e27d4fe3394"}},
                    {"id": "cddf9b8f-27f9-4fe9-97cb-9e27d4fe3394", "imdb_rating": 7.9, "genre": ["Action", "Adventure", "Sci-Fi"], "title": "Star Wars: Episode VII - The Force Awakens", "description": "30 years after the defeat of Darth Vader and the Empire, Rey, a scavenger from the planet Jakku, finds a BB-8 droid that knows the whereabouts of the long lost Luke Skywalker. Rey, as well as a rogue stormtrooper and two smugglers, are thrown into the middle of a battle between the Resistance and the daunting legions of the First Order.", "director": ["J.J. Abrams"], "actors_names": ["Adam Driver", "Carrie Fisher", "Harrison Ford", "Mark Hamill"], "writers_names": ["George Lucas", "J.J. Abrams", "Lawrence Kasdan", "Michael Arndt"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "3b914679-1f5e-4cbd-8044-d13d35d5236c"}},
                    {"id": "3b914679-1f5e-4cbd-8044-d13d35d5236c", "imdb_rating": 6.5, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode I - The Phantom Menace", "description": "The evil Trade Federation, led by Nute Gunray is planning to take over the peaceful world of Naboo. Jedi Knights Qui-Gon Jinn and Obi-Wan Kenobi are sent to confront the leaders. But not everything goes to plan. The two Jedi escape, and along with their new Gungan friend, Jar Jar Binks head to Naboo to warn Queen Amidala, but droids have already started to capture Naboo and the Queen is not safe there. Eventually, they land on Tatooine, where they become friends with a young boy known as Anakin Skywalker. Qui-Gon is curious about the boy, and sees a bright future for him. The group must now find a way of getting to Coruscant and to finally solve this trade dispute, but there is someone else hiding in the shadows. Are the Sith really extinct? Is the Queen really who she says she is? And what's so special about this young boy?", "director": ["George Lucas"], "actors_names": ["Ewan McGregor", "Jake Lloyd", "Liam Neeson", "Natalie Portman"], "writers_names": ["George Lucas"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "516f91da-bd70-4351-ba6d-25e16b7713b7"}},
                    {"id": "516f91da-bd70-4351-ba6d-25e16b7713b7", "imdb_rating": 7.5, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode III - Revenge of the Sith", "description": "Near the end of the Clone Wars, Darth Sidious has revealed himself and is ready to execute the last part of his plan to rule the galaxy. Sidious is ready for his new apprentice, Darth Vader, to step into action and kill the remaining Jedi. Vader, however, struggles to choose the dark side and save his wife or remain loyal to the Jedi order.", "director": ["George Lucas"], "actors_names": ["Ewan McGregor", "Hayden Christensen", "Ian McDiarmid", "Natalie Portman"], "writers_names": ["George Lucas"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "c4c5e3de-c0c9-4091-b242-ceb331004dfd"}},
                    {"id": "c4c5e3de-c0c9-4091-b242-ceb331004dfd", "imdb_rating": 6.5, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode II - Attack of the Clones", "description": "Ten years after the invasion of Naboo, the Galactic Republic is facing a Separatist movement and the former queen and now Senator Padm\u00e9 Amidala travels to Coruscant to vote on a project to create an army to help the Jedi to protect the Republic. Upon arrival, she escapes from an attempt to kill her, and Obi-Wan Kenobi and his Padawan Anakin Skywalker are assigned to protect her. They chase the shape-shifter Zam Wessell but she is killed by a poisoned dart before revealing who hired her. The Jedi Council assigns Obi-Wan Kenobi to discover who has tried to kill Amidala and Anakin to protect her in Naboo. Obi-Wan discovers that the dart is from the planet Kamino, and he heads to the remote planet. He finds an army of clones that has been under production for years for the Republic and that the bounty hunter Jango Fett was the matrix for the clones. Meanwhile Anakin and Amidala fall in love with each other, and he has nightmarish visions of his mother. They travel to his home planet, Tatooine, to see his mother, and he discovers that she has been abducted by Tusken Raiders. Anakin finds his mother dying, and he kills all the Tusken tribe, including the women and children. Obi-Wan follows Jango Fett to the planet Geonosis where he discovers who is behind the Separatist movement. He transmits his discoveries to Anakin since he cannot reach the Jedi Council. Who is the leader of the Separatist movement? Will Anakin receive Obi-Wan's message? And will the secret love between Anakin and Amidala succeed?", "director": ["George Lucas"], "actors_names": ["Christopher Lee", "Ewan McGregor", "Hayden Christensen", "Natalie Portman"], "writers_names": ["George Lucas", "Jonathan Hales"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "4af6c9c9-0be0-4864-b1e9-7f87dd59ee1f"}},
                    {"id": "4af6c9c9-0be0-4864-b1e9-7f87dd59ee1f", "imdb_rating": 7.9, "genre": ["Action", "Adventure", "Sci-Fi"], "title": "Star Trek", "description": "On the day of James Kirk's birth, his father dies on his damaged starship in a last stand against a Romulan mining vessel looking for Ambassador Spock, who in this time, has grown on Vulcan disdained by his neighbors for his half-human heritage. 25 years later, James T. Kirk has grown into a young rebellious troublemaker. Challenged by Captain Christopher Pike to realize his potential in Starfleet, he comes to annoy academy instructors like Commander Spock. Suddenly, there is an emergency on Vulcan and the newly-commissioned USS Enterprise is crewed with promising cadets like Nyota Uhura, Hikaru Sulu, Pavel Chekov and even Kirk himself, thanks to Leonard McCoy's medical trickery. Together, this crew will have an adventure in the final frontier where the old legend is altered forever as a new version of the legend begins.", "director": ["J.J. Abrams"], "actors_names": ["Chris Pine", "Eric Bana", "Leonard Nimoy", "Zachary Quinto"], "writers_names": ["Alex Kurtzman", "Gene Roddenberry", "Roberto Orci"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "12a8279d-d851-4eb9-9d64-d690455277cc"}},
                    {"id": "12a8279d-d851-4eb9-9d64-d690455277cc", "imdb_rating": 7.0, "genre": ["Action", "Adventure", "Fantasy", "Sci-Fi"], "title": "Star Wars: Episode VIII - The Last Jedi", "description": "Rey develops her newly discovered abilities with the guidance of Luke Skywalker, who is unsettled by the strength of her powers. Meanwhile, the Resistance prepares for battle with the First Order.", "director": ["Rian Johnson"], "actors_names": ["Adam Driver", "Carrie Fisher", "Daisy Ridley", "Mark Hamill"], "writers_names": ["George Lucas", "Rian Johnson"], "actors": {"id": "name"}, "writers": {"id": "name"}},
                    {"index": {"_index": "example_index", "_id": "118fd71b-93cd-4de5-95a4-e1485edad30e"}},
                    {"id": "118fd71b-93cd-4de5-95a4-e1485edad30e", "imdb_rating": 7.8, "genre": ["Action", "Adventure", "Sci-Fi"], "title": "Rogue One: A Star Wars Story", "description": "All looks lost for the Rebellion against the Empire as they learn of the existence of a new super weapon, the Death Star. Once a possible weakness in its construction is uncovered, the Rebel Alliance must set out on a desperate mission to steal the plans for the Death Star. The future of the entire galaxy now rests upon its success.", "director": ["Gareth Edwards"], "actors_names": ["Alan Tudyk", "Diego Luna", "Donnie Yen", "Felicity Jones"], "writers_names": ["Chris Weitz", "Gary Whitta", "George Lucas", "John Knoll", "Tony Gilroy"], "actors": {"id": "name"}, "writers": {"id": "name"}}

               ]
            )
print(resp)