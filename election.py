import pandas as pd
import time

def assign_votes():
    for i, indice in enumerate(voters.index):
        candidate = voters.loc[indice].votes[voters.loc[indice].pref]
        value = voters.loc[indice].value
        previous = candidates.loc[candidate, "votes"]
        candidates.loc[candidate, "votes"] = previous + 1
    candidates.sort_values("votes", ascending=False, inplace=True)
    record_primary_votes()
    check_elected()

def record_primary_votes():
    total_votes = candidates.votes.sum()
    for candidate in candidates.index:
        candidates.loc[candidate, "primary"] = f"{candidates.loc[candidate].votes / total_votes:.1%}"

def check_elected():
    global count_no
    print(f"\n***** COUNT {count_no} *****")
    reached_quota = candidates[candidates["votes"] >= quota]
    if len(reached_quota) == 0:
        print("no candidates elected")
        count_no = count_no + 1
        # select eliminated candidate
        candidate = candidates.index[len(candidates) - 1]
        name = candidates.loc[candidate].cname
        party = candidates.loc[candidate].party
        primary = candidates.loc[candidate].primary
        print(f"{name} is eliminated!")
        eliminated_candidates.loc[candidate] = [name, party, primary]
        eliminate_candidate(candidate)
    else:
        candidate = reached_quota.index[0]
        name = candidates.loc[candidate].cname
        party = candidates.loc[candidate].party
        primary = candidates.loc[candidate].primary
        print(f"{name} is elected!")
        elected_candidates.loc[candidate] = [name, party, primary]
        if len(elected_candidates) < 5:
            count_no = count_no + 1
            redistribute_excess_votes(candidate)
        else:
            print("\nELECTED")
            print(elected_candidates)
            for candidate in candidates.index:
                name = candidates.loc[candidate].cname
                party = candidates.loc[candidate].party
                primary = candidates.loc[candidate].primary
                eliminated_candidates.loc[candidate] = [name, party, primary]
            print("\nELIMINATED")
            print(eliminated_candidates)
            print(f"\nexhausted votes: {exhausted_votes:,.1f}")            

def redistribute_excess_votes(candidate):    
    votes = candidates.loc[candidate].votes
    fraction = votes / quota - 1
    name = candidates.loc[candidate].cname
    print(f"redistributing {name}'s {votes:.0f} votes at a fractional value of {fraction:.3f} ...")
    reallocate_votes(candidate, fraction)
    check_elected()

def eliminate_candidate(candidate):
    votes = candidates.loc[candidate].votes
    name = candidates.loc[candidate].cname
    print(f"redistributing {name}'s {votes:.1f} votes ...")
    reallocate_votes(candidate, 1)
    check_elected()

def reallocate_votes(candidate, fraction):
    global candidates, exhausted_votes, voters
    # iterrate through votes
    for i, indice in enumerate(voters.index):
        pref = voters.loc[indice].pref
        # find votes for elected candidate
        if voters.loc[indice].votes[pref] == candidate:
            value = voters.loc[indice].value
            while True:
                pref = pref + 1                
                voters.loc[indice].pref = pref                
                # is it possible to distribute the vote?
                if pref > len(voters.loc[indice].votes) - 1:
                    exhausted_votes = exhausted_votes + value * fraction                    
                    voters = voters[voters.index != indice]
                    break
                else:
                    new_candidate = voters.loc[indice].votes[pref]
                    if (new_candidate in eliminated_candidates.index) or (new_candidate in elected_candidates.index):
                        pass                    
                    else:                    
                        candidates.loc[new_candidate, "votes"] = candidates.loc[new_candidate].votes + value * fraction
                        break
    print(f"exhausted votes: {exhausted_votes:,.1f}")
    candidates = candidates[candidates.index != candidate]
    candidates.sort_values("votes", ascending=False, inplace=True)

def create_ids(df):
    start_time = time.time()
    time_interval = 10
    ids = []
    for i, indice in enumerate(df.index):
        ids.append(f"{df.loc[indice].pcode}-{df.loc[indice].ccode}")
        if (time.time() - start_time) > time_interval:
            print(f"{(i + 1) / len(df.index):.1%} processed")
            time_interval = time_interval + 10
    df["id"] = ids
    print("complete")
    return df

# BEGIN PROGRAM
    
print("2020 ACT election simulator — Yerrabi")

# read in data
print("\nreading in data ...")
groups = pd.read_csv("./data/Groups.txt")
candidates = pd.read_csv("./data/Candidates.txt")
yerrabi = pd.read_csv("./data/YerrabiTotal.txt", usecols=["pindex", "pref", "pcode", "ccode"])
print("complete")

# create temporary vote sample
# yerrabi = yerrabi[175000:200000]

print("\nfiltering data ...")

# limit datasets to Yerrabi-only
groups = groups[groups["ecode"] == 5].set_index("pcode")
candidates = candidates[candidates["ecode"] == 5]
print("complete")

print("\ncreating candidate IDs ...")

# create candidate IDs
candidates["votes"] = [0] * len(candidates)
candidates["primary"] = [0] * len(candidates)
candidates["cname"] = candidates["cname"].apply(lambda x: x.split(", ")[1] + " " + x.split(", ")[0])
candidates["party"] = candidates["pcode"].apply(lambda x: groups.loc[x].pabbrev)
candidates = create_ids(candidates)
candidates.set_index("id", inplace=True)
candidates = candidates[["cname", "party", "primary", "votes"]]

print("\npreparing preference data ...")

# assign candidate IDs to each vote preference
yerrabi = create_ids(yerrabi)
yerrabi[["pindex", "pref", "pcode", "id"]]

print("\ncreating voter files ...")

# group vote preferences into individual voters
voters = pd.DataFrame(index=yerrabi.pindex.unique(), columns=["votes", "pref", "value"])
start_time = time.time()
time_interval = 10
for i, indice in enumerate(voters.index):
    data = yerrabi[yerrabi["pindex"] == indice].sort_values("pref")
    votes = []
    for j in data.index:
        votes.append(data.loc[j].id)
    voters.loc[indice].votes = votes
    voters.loc[indice].pref = 0
    voters.loc[indice].value = 1
    if (time.time() - start_time) > time_interval:
        print(f"{(i + 1) / len(voters.index):.1%} processed")
        time_interval = time_interval + 10
print("complete")

quota = len(voters) / 6 + 1
print(f"\nquota established: {quota:.1f}")

print("\ncreate final lists")

elected_candidates = pd.DataFrame(columns=["cname", "party", "primary"])
eliminated_candidates = pd.DataFrame(columns=["cname", "party", "primary"])
exhausted_votes = 0
print("complete")

print("\ndistribute initial votes ...")

# begin counting rounds
count_no = 1
assign_votes()
