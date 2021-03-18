import pandas as pd
import time
pd.options.mode.chained_assignment = None

# functions
def get_electorate():
    ''' select electorate from text menu '''
    valid_response = False
    while not valid_response:
        try:
            print("\nWhich electorate do you wish to analyse?")
            for i, electorate in enumerate(electorates.electorate):
                print(f"{i + 1}. {electorate}")
            print("6. quit program")
            text = input("Enter a number from 1 to 6: ")
            text = int(text)
            if (text > 0) and (text < 7):
                valid_response = True
                return text
            else:
                print("That's not a valid choice.")
        except:
            print("That's not a valid choice.")

def get_sample_size():
    ''' select valid sample size (0 > sample_size ≥ 1) '''
    valid_response = False
    while not valid_response:
        try:
            text = input("How large is the sample (0 to 1): ")
            text = float(text)
            if (text > 0) and (text <= 1):
                valid_response = True
                return text
            else:
                print("That's not a valid choice.")
        except:
            print("That's not a valid choice.")

def eliminate_candidate():
    ''' record eliminated candidate and return ID '''
    global eliminated
    candidate = candidates.index[len(candidates) - 1]
    name = candidates.at[candidate, "cname"]
    party = candidates.at[candidate, "party"]
    primary = candidates.at[candidate, "primary"]
    votes = candidates.at[candidate, "votes"]
    eliminated.loc[candidate] = [name, party, primary, votes]
    print(f"{name} is eliminated!")
    return candidate

def elect_candidate(candidate):
    ''' record elected candidate '''
    global elected
    name = candidates.at[candidate, "cname"]
    print(f"{name} is elected!")
    party = candidates.at[candidate, "party"]
    primary = candidates.at[candidate, "primary"]
    votes = candidates.at[candidate, "votes"]
    elected.loc[candidate] = [name, party, primary, votes]
    print(f"{name} had {candidates.at[candidate, 'votes']:,.1f} votes.")

def calculate_transfer_value(candidate):
    ''' returns the transfer value of elected candidates' votes '''
    print(f"calculating transfer value of {candidates.at[candidate, 'cname']}'s votes ...", end="\r")
    count_votes = []
    start = time.time()
    interval = 1
    elapsed = interval
    for i, indice in enumerate(sample.index):
        # is vote for this candidate?
        if sample.at[indice, "votes"][sample.at[indice, "pref"]] == candidate:
            # is there a next preference?            
            preference_resolved = False
            pref_next = 1
            while not preference_resolved:
                if len(sample.at[indice, "votes"]) > sample.at[indice, "pref"] + pref_next:                    
                    # is preferenced candidate still in count?
                    if sample.at[indice, "votes"][sample.at[indice, "pref"] + pref_next] in candidates.index:
                        # add vote value to collection
                        count_votes.append(sample.at[indice, "value"])
                        preference_resolved = True
                    else:
                        pref_next = pref_next + 1
                else:
                    preference_resolved = True
        if (time.time() - elapsed) > start:
            print(f"calculating transfer value of {candidates.at[candidate, 'cname']}'s votes ... {(i + 1) / len(sample):.1%}", end="\r")
            elapsed = elapsed + interval
    surplus = candidates.at[candidate, "votes"] - quota
    transfer_value = round(surplus / sum(count_votes), 6) if sum(count_votes) else 0.0
    print(f"calculating transfer value of {candidates.at[candidate, 'cname']}'s votes ... complete")
    print(f"transfer value: {transfer_value}")
    return transfer_value
    
def distribute_votes(candidate, victory):
    ''' redistributes votes/surplus votes from eliminated/elected candidates '''
    global candidates, sample, exhausted
    # calculate transfer value if candidate is elected
    if victory:
        transfer_value = calculate_transfer_value(candidate)
    print(f"redistributing {candidates.at[candidate, 'cname']}'s {'surplus ' if victory else ''}votes ...", end="\r")
    start = time.time()
    interval = 1
    elapsed = interval
    sample_length = len(sample)
    for i, indice in enumerate(sample.index):
        # is vote for this candidate?
        if sample.at[indice, "votes"][sample.at[indice, "pref"]] == candidate:
            if victory:
                sample.at[indice, "value"] = round(sample.at[indice, "value"] * transfer_value, 6)
            # remove vote from eliminated/elected candidate
            candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"] - sample.at[indice, "value"]
            # is there a next preference?
            preference_resolved = False
            while not preference_resolved:
                if len(sample.at[indice, "votes"]) > sample.at[indice, "pref"] + 1:
                    pref = sample.at[indice, "pref"] + 1
                    sample.at[indice, "pref"] = pref
                    votes = sample.at[indice, "votes"]
                    value = sample.at[indice, "value"]
                    # is preferenced candidate still in count?
                    if votes[pref] in candidates.index:
                        # add vote to preferenced candidate
                        candidates.at[votes[pref], "votes"] = candidates.at[votes[pref], "votes"] + value
                        preference_resolved = True
                # add to exhausted votes
                else:                
                    exhausted = exhausted + sample.at[indice, "value"]
                    sample.drop(index=indice, inplace=True)
                    preference_resolved = True
        if (time.time() - elapsed) > start:
            print(f"redistributing {candidates.at[candidate, 'cname']}'s {'surplus ' if victory else ''}votes ... {(i + 1) / sample_length:.1%}", end="\r")
            elapsed = elapsed + interval
    if victory:
        elected.at[candidate, "votes"] = candidates.at[candidate, "votes"]
    else:
        eliminated.at[candidate, "votes"] = candidates.at[candidate, "votes"]
    print(f"redistributing {candidates.at[candidate, 'cname']}'s {'surplus ' if victory else ''}votes ... complete")

def record_count(event):
    ''' records current vote tally and saves to file '''
    global results_phase, results, elected, eliminated
    results_phase = results_phase + 1
    results.at[results_phase, "event"] = event
    results.at[results_phase, "exhausted"] = exhausted
    for candidate in elected.index:
        final_votes = elected.at[candidate, "votes"]
        if final_votes > quota:
            elected.at[candidate, "votes"] = quota
        results.at[results_phase, elected.at[candidate, "cname"]] = quota
    for candidate in eliminated.index:
        final_votes = eliminated.at[candidate, "votes"]
        if final_votes > 0:
            eliminated.at[candidate, "votes"] = 0.0
        results.at[results_phase, eliminated.at[candidate, "cname"]] = 0.0
    for candidate in candidates.index:
        results.at[results_phase, candidates.at[candidate, "cname"]] = candidates.at[candidate, "votes"]
    results.at[results_phase, "total votes"] = results.loc[results_phase][2:].sum()
    results.to_csv(f"./data/results_{electorates.at[electorate, 'electorate']}.csv")

# BEGIN PROGRAM

print("2020 ACT ELECTION SIMULATOR")
print("\u00a9 Markus Mannheim (ABC Canberra)")

# read in electorates
electorates = pd.read_csv("./data/Electorates.txt", index_col="ecode")

# begin cycle
while True:
    electorate = get_electorate()
   
    # user wants to quit
    if electorate == 6:
        break

    sample_size = get_sample_size()
        
    # load electorate data
    print(f"\nloading {electorates.at[electorate, 'electorate']} data ...", end="\r")
    candidates = pd.read_csv(f"./data/candidates_{electorates.at[electorate, 'electorate']}.csv", index_col="id")
    parties = pd.read_csv(f"./data/parties_{electorates.at[electorate, 'electorate']}.csv", index_col="id")
    votes = pd.read_csv(f"./data/votes_{electorates.at[electorate, 'electorate']}.csv", index_col="identifier")
    votes.votes = votes.votes.apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").split(", "))
    print(f"loading {electorates.at[electorate, 'electorate']} data ... complete")
    
    # create sample data to speed up calculation
    print("sampling data for analysis ...", end="\r")
    sample = votes.sample(frac=sample_size)
    print("sampling data for analysis ... complete")
 
    # prepare datasets to contain elected and eliminated candidates
    print("preparing results containers ...", end="\r")
    elected = pd.DataFrame(columns=["cname", "party", "primary", "votes"])
    eliminated = pd.DataFrame(columns=["cname", "party", "primary", "votes"])
    exhausted = 0.0
    results = pd.DataFrame(columns=["event", "total votes", "exhausted"] + list(candidates.cname))
    results.index.name = "phase"
    print("preparing results containers ... complete")

    # establish quota
    total_votes = len(sample)
    quota = int(total_votes / 6) + 1
    print(f"\nquota for {electorates.at[electorate, 'electorate']}: {format(quota, ',')} votes")

    # record primary votes
    print("recording primary votes ...", end="\r")
    start = time.time()
    interval = 1
    elapsed = interval
    for i, indice in enumerate(sample.index):
        candidate = sample.at[indice, "votes"][0]
        candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"] + 1
        if (time.time() - elapsed) > start:
            print(f"recording primary votes ... {(i + 1) / len(sample):.1%}", end="\r")
    candidates.primary = candidates.votes.apply(lambda x: f"{x / total_votes:.1%}")
    candidates.sort_values("votes", ascending=False, inplace=True)
    
    # begin counting cycle
    count_no = 1
    results_phase = 0
    record_count("primary votes")
    print()
    while len(elected) < 5:
        print(f"\nCOUNT No. {count_no}\n")
        # check if candidates elected
        reached_quota = candidates[candidates["votes"] >= quota]
        if len(reached_quota) == 0:
            print("no candidates elected")
            # check is this the last candidate?
            if len(elected) == 4:                
                # are any candidates left?
                if len(candidates) > 0:
                    print("only one candidate is left ...")
                    candidate = candidates.index[0]
                else:
                    print("all candidates already eliminated ...")
                    candidate = eliminated.index[::-1]
                    candidates.loc[candidate] = eliminated.loc[candidate]
                    eliminated.drop(index=candidate, inplace=True)
                elect_candidate(candidate)
                record_count(f"{elected.at[candidate, 'cname']} elected by default")
                candidates.drop(index=candidate, inplace=True)
            else:
                # eliminate last-placed candidate
                eliminated_candidate = eliminate_candidate()
                distribute_votes(eliminated_candidate, False)            
                record_count(f"{eliminated.at[eliminated_candidate, 'cname']} eliminated")
                candidates.drop(index=eliminated_candidate, inplace=True)
        else:
            # iterate through newly elected candidates
            i = 0
            while len(elected) < 5:
                candidate = reached_quota.index[i]
                elect_candidate(candidate)
                if (len(elected) < 5):
                    distribute_votes(candidate, True)
                record_count(f"{elected.at[candidate, 'cname']} elected")
                candidates.drop(index=candidate, inplace=True)
                i = i + 1
                if len(reached_quota) < i + 1:
                    break
        candidates.sort_values("votes", ascending=False, inplace=True)
        count_no = count_no + 1
    # simulation results
    print()
    print(elected[["cname", "party", "primary"]])

# exit program
print("\nEnjoy your day.")