import pandas as pd
import time

# functions
def get_electorate():
    ''' select electorate from text menu '''       
    valid_response = True
    while valid_response:
        try:
            print("\nWhich electorate do you wish to analyse?")
            for i, electorate in enumerate(electorates.electorate):
                print(f"{i + 1}. {electorate}")    
            print("6. quit program")
            text = input("Enter a number from 1 to 5:")            
            text = int(text) - 1
            valid_response = True
            return text
        except:
            print("That's not a valid choice.")

def create_ids(label, df):
    ''' creates unique IDs and sets them as the dataframe index '''
    start_time = time.time()
    time_interval = 5
    ids = []
    for i, indice in enumerate(df.index):
        ids.append(f"{df.loc[indice].pcode}-{df.loc[indice].ccode}")
        if (time.time() - start_time) > time_interval:
            print(f"creating {label} IDs ... {(i + 1) / len(df):.1%} processed", end="\r")
            time_interval = time_interval + 10
    df["id"] = ids
    print(f"creating {label} IDs ... complete")
    return df
            
def format_candidates(data):
    ''' format the candidates dataframe '''
    data["votes"] = 0
    data["primary"] = 0
    data["cname"] = data["cname"].apply(lambda x: x.split(", ")[1] + " " + x.split(", ")[0])
    data["party"] = data["pcode"].apply(lambda x: active_parties.loc[x].pabbrev)
    data = create_ids("candidate", data)
    data.set_index("id", inplace=True)
    data = data[["cname", "party", "primary", "votes"]]
    return data            
            
# BEGIN PROGRAM

if __name__ == "__main__":
    print("2020 ACT ELECTION SIMULATOR")
    print("\u00a9 Markus Mannheim (ABC Canberra)")
    
    # read in parameters
    print("\nestablishing databases ...", end=" ")
    parties = pd.read_csv("./data/Groups.txt")
    candidates = pd.read_csv("./data/Candidates.txt")
    electorates = pd.read_csv("./data/Electorates.txt")
    print("complete")

    # read in votes
    print("reading in ballot papers ...", end=" ")
    ballots = pd.DataFrame(columns=["pindex", "pref", "pcode", "ccode", "ecode"])
    for electorate in electorates.index:    
        add_ballots = pd.read_csv("./data/YerrabiTotal.txt", usecols=["pindex", "pref", "pcode", "ccode"])
        add_ballots["ecode"] = electorates.loc[electorate].ecode
        ballots = ballots.append(add_ballots)
    print("complete")

    # begin cycle    
    while True:
        electorate = get_electorate()
        # user wants to quit
        if electorate == 5:
            break
        
        # activate electorate simulation
        
        # filter data to chosen electorate
        print(f"\nfiltering {electorates.loc[electorate].electorate} data ...", end=" ")
        active_ballots = ballots[ballots["ecode"] == electorate]
        active_candidates = candidates[candidates["ecode"] == electorate]
        active_parties = parties[parties["ecode"] == electorate].set_index("pcode")
        print("complete")
        
        # creat sample data to limit speed up calculation
        print(f"sampling data for analysis...", end=" ")
        sample_ballots = active_ballots.sample(frac=.1, random_state=2)
        print("complete")

        # format candidate and ballot data
        active_candidates = format_candidates(active_candidates)
        print()
        print(active_candidates)

        # print("\npreparing preference data ...")

        # # assign candidate IDs to each vote preference
        # yerrabi = create_ids(yerrabi)
        # yerrabi[["pindex", "pref", "pcode", "id"]]

        # print("\ncreating voter files ...")
        
        
    
    # exit program
    print("\nEnjoy your day.")
    # exit()