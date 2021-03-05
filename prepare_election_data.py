import pandas as pd
import time
pd.options.mode.chained_assignment = None

def format_parties(ecode, electorate):
    print(f"\nformatting party data for {electorate} ...", end="\r")
    filtered_data = parties[parties.ecode == ecode][["pcode", "pname", "pabbrev"]].set_index("pcode")
    print(f"formatting party data for {electorate} ... complete")
    return filtered_data

def format_candidates(ecode, electorate):    
    print(f"formatting candidate data for {electorate} ...", end="\r")
    filtered_data = candidates[candidates.ecode == ecode]
    ids, party_list = [], []    
    for indice in filtered_data.index:
        id = str(filtered_data.at[indice, "pcode"]) + "-" + str(filtered_data.at[indice, "ccode"])
        ids.append(id)
        party = active_parties.at[filtered_data.at[indice, "pcode"], "pabbrev"]
        party_list.append(party)
    filtered_data["id"] = ids
    filtered_data["party"] = party_list
    filtered_data["primary"] = 0
    filtered_data["votes"] = 0
    filtered_data.set_index("id", inplace=True)
    filtered_data["cname"] = filtered_data["cname"].apply(lambda x: x.split(", ")[1] + " " + x.split(", ")[0])
    print(f"formatting candidate data for {electorate} ... complete")
    return filtered_data[["cname", "party", "primary", "votes"]]

def format_ballots(ecode, electorate):
    print(f"formatting ballots for {electorate} ...", end="\r")
    filtered_data = pd.read_csv(f"./data/{electorate}Total.txt", usecols=["pindex", "pref", "pcode", "ccode"])
    ids = []
    start = time.time()
    interval = 1
    elapsed = interval
    for i, indice in enumerate(filtered_data.index):
        id = str(filtered_data.at[indice, "pcode"]) + "-" + str(filtered_data.at[indice, "ccode"])
        ids.append(id)
        if (time.time() - elapsed) > start:
            print(f"formatting ballots for {electorate} ... {(i + 1) / len(filtered_data):.1%}", end="\r")
            elapsed = elapsed + interval
    filtered_data["id"] = ids    
    print(f"formatting ballots for {electorate} ... complete")
    return filtered_data

def create_votes(ecode, electorate):
    print(f"creating vote files for {electorate} ...", end="\r")
    filtered_data = pd.DataFrame(index=active_ballots.pindex.unique(), columns=["votes", "pref", "value"])
    start = time.time()
    interval = 1
    elapsed = interval
    for i, vote in enumerate(filtered_data.index):
        data = active_ballots[active_ballots.pindex == vote].sort_values("pref")
        votes = []
        for j in data.index:
            votes.append(data.at[j, "id"])
        filtered_data.at[vote, "votes"] = votes
        filtered_data.at[vote, "pref"] = 0
        filtered_data.at[vote, "value"] = 1
        if (time.time() - start) > elapsed:
            print(f"creating vote files for {electorate} ... {(i + 1) / len(filtered_data):.1%}", end="\r")
            elapsed = elapsed + interval
    print(f"creating vote files for {electorate} ... complete")
    return filtered_data

# BEGIN PROGRAM

print("2020 ACT ELECTION DATA FORMATTER")
print("\u00a9 Markus Mannheim (ABC Canberra)")

# read in parameters
print("\nreading in election parameters ...", end="\r")
electorates = pd.read_csv("./data/Electorates.txt", index_col="ecode")
parties = pd.read_csv("./data/Groups.txt")
candidates = pd.read_csv("./data/Candidates.txt")
ballots = pd.DataFrame(columns=["pindex", "pref", "pcode", "ccode", "ecode"])
print("reading in election parameters ... complete")

# begin electorate cycle
for ecode, edata in electorates.iterrows():    
    active_parties = format_parties(ecode, edata.electorate)
    active_candidates = format_candidates(ecode, edata.electorate)
    active_ballots = format_ballots(ecode, edata.electorate)
    active_votes = create_votes(ecode, edata.electorate)

    # save data
    print(f"saving data for {edata.electorate} ...", end="\r")
    active_candidates.to_csv(f"./data/candidates_{edata.electorate}.csv", index_label="id")
    active_parties.to_csv(f"./data/parties_{edata.electorate}.csv", index_label="id")
    active_votes.to_csv(f"./data/votes_{edata.electorate}.csv", index_label="id")
    print(f"saving data for {edata.electorate} ... complete")