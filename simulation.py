import pandas as pd
import time
pd.options.mode.chained_assignment = None

# FUNCTIONS

def get_electorate():
    ''' select electorate from menu '''
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

def record_count(event):
    ''' saves current vote tallies to file '''
    global results_phase, results, elected, eliminated, fractions
    results_phase = results_phase + 1
    results.at[results_phase, "event"] = event
    results.at[results_phase, "exhausted"] = exhausted
    results.at[results_phase, "fractions"] = fractions
    for candidate in elected.index:
        results.at[results_phase, elected.at[candidate, "cname"]] = elected.at[candidate, "votes"]
    for candidate in eliminated.index:
        results.at[results_phase, eliminated.at[candidate, "cname"]] = eliminated.at[candidate, "votes"]
    for candidate in candidates.index:
        results.at[results_phase, candidates.at[candidate, "cname"]] = candidates.at[candidate, "votes"]
    results.at[results_phase, "total votes"] = round(results.loc[results_phase][2:].sum(), 6)
    results.to_csv(f"./data/results_{electorates.at[electorate, 'electorate']}.csv")

def elect_candidate(candidate):
    ''' record elected candidate '''
    global elected
    name = candidates.at[candidate, "cname"]
    print(f"{name} is elected!")
    party = candidates.at[candidate, "party"]
    primary = candidates.at[candidate, "primary"]
    votes = candidates.at[candidate, "votes"]
    elected.loc[candidate] = [name, party, primary, votes]
    record_count(f"{name} elected by default")
    print(f"{name} had {candidates.at[candidate, 'votes']:,.1f} votes.")

def eliminate_candidate():
    ''' record eliminated candidate and return ID '''
    global eliminated
    candidate = candidates.index[len(candidates) - 1]
    name = candidates.at[candidate, "cname"]
    party = candidates.at[candidate, "party"]
    primary = candidates.at[candidate, "primary"]
    votes = candidates.at[candidate, "votes"]
    eliminated.loc[candidate] = [name, party, primary, votes]
    record_count(f"{name} eliminated")
    print(f"{name} is eliminated!")
    return candidate

def distribute_eliminated_votes(candidate):
    ''' distribute votes from eliminated/elected candidate '''
    global candidates, sample, exhausted, temp_sample
    print(f"distributing {eliminated.at[candidate, 'cname']}'s votes ...", end="\r")
    start = time.time()
    interval = 1
    elapsed = interval
    sample_length = len(sample)
    for i, indice in enumerate(sample.index):
        # hold vote in transition
        temp_sample.loc[indice] = sample.loc[indice]
        # is vote for this candidate?
        if sample.at[indice, "votes"][sample.at[indice, "pref"]] == candidate:
            # is there a next preference?
            preference_resolved = False
            votes = sample.at[indice, "votes"]
            value = sample.at[indice, "value"]
            while len(sample.at[indice, "votes"]) > sample.at[indice, "pref"] + 1:
                pref = sample.at[indice, "pref"] + 1
                sample.at[indice, "pref"] = pref
                # is preferenced candidate still in count?
                if (votes[pref] not in elected.index) and (votes[pref] not in eliminated.index) and  (votes[pref] not in reached_quota.index):
                    # add vote to preferenced candidate
                    candidates.at[votes[pref], "votes"] = candidates.at[votes[pref], "votes"] + value
                    # remove vote from eliminated candidate
                    candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"] - value
                    preference_resolved = True
                    break
            # add to exhausted votes
            if not preference_resolved:                
                exhausted = exhausted + value
                candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"] - value
                sample.drop(index=indice, inplace=True)
                temp_sample.drop(index=indice, inplace=True)
        if (time.time() - elapsed) > start:
            print(f"distributing {eliminated.at[candidate, 'cname']}'s votes ... {(i + 1) / sample_length:.1%}", end="\r")
            elapsed = elapsed + interval
    eliminated.at[candidate, "votes"] = candidates.at[candidate, "votes"]    
    record_count(f"{eliminated.at[candidate, 'cname']}'s votes distributed")
    candidates.drop(index=candidate, inplace=True)
    print(f"distributing {eliminated.at[candidate, 'cname']}'s votes ... complete")

def distribute_surplus_votes(candidate):
    ''' distribute surplus votes at fractional value from elected candidate '''
    global candidates, temp_sample, sample, exhausted, fractions
    print(f"calculating transfer value for {elected.at[candidate, 'cname']}'s surplus votes ...") #, end="\r")
    # calculate transfer value
    start = time.time()
    interval = 1
    elapsed = interval
    temp_sample_length = len(temp_sample)
    valid_votes = pd.DataFrame(columns=temp_sample.columns)
    for i, indice in enumerate(temp_sample.index):
        # is vote for this candidate?
        if temp_sample.at[indice, "votes"][temp_sample.at[indice, "pref"]] == candidate:
            # is there a next preference?
            votes = temp_sample.at[indice, "votes"]
            value = temp_sample.at[indice, "value"]
            while len(temp_sample.at[indice, "votes"]) > temp_sample.at[indice, "pref"] + 1:
                temp_sample.at[indice, "pref"] = temp_sample.at[indice, "pref"] + 1
                pref = temp_sample.at[indice, "pref"]
                # is preferenced candidate still in count?
                if (votes[pref] not in elected.index) & (votes[pref] not in eliminated.index) & (votes[pref] not in reached_quota.index):
                    # add vote to valid votes                    
                    valid_votes.loc[indice] = temp_sample.loc[indice]
                    break
        if (time.time() - elapsed) > start:
            print(f"calculating transfer value for {elected.at[candidate, 'cname']}'s surplus votes ... {(i + 1) / temp_sample_length:.1%}", end="\r")
            elapsed = elapsed + interval
    surplus = candidates.at[candidate, "votes"] - quota
    valid_value = valid_votes.value.sum()
    transfer_value = min(round(surplus / valid_value, 6), 1) if valid_value > 0 else 0.0
    print(f"calculating transfer value for {elected.at[candidate, 'cname']}'s surplus votes ... complete")
    
    # allocate valid votes
    valid_votes.value = round(valid_votes.value * transfer_value, 6)
    fraction_change = round(surplus - valid_votes.value.sum(), 6)
    fractions = fractions + fraction_change
    candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"] - fraction_change
    print(f"distributing {elected.at[candidate, 'cname']}'s surplus votes ...", end="\r")
    start = time.time()
    interval = 1
    elapsed = interval
    valid_votes_length = len(valid_votes)
    candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"]
    for i, indice in enumerate(valid_votes.index):
        pref = valid_votes.at[indice, "pref"]
        value = valid_votes.at[indice, "value"]
        recipient = valid_votes.at[indice, "votes"][pref]
        candidates.at[recipient, "votes"] = candidates.at[recipient, "votes"] + value
        candidates.at[candidate, "votes"] = candidates.at[candidate, "votes"] - value
        temp_sample.drop(index=indice, inplace=True)
        if (time.time() - elapsed) > start:
            print(f"distributing {elected.at[candidate, 'cname']}'s surplus votes ... {(i + 1) / valid_votes_length:.1%}", end="\r")
            elapsed = elapsed + interval
    elected.at[candidate, "votes"] = candidates.at[candidate, "votes"]
    record_count(f"{elected.at[candidate, 'cname']}'s surplus distributed")
    candidates.drop(index=candidate, inplace=True)
    print(f"distributing {elected.at[candidate, 'cname']}'s surplus votes ... complete")
    
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
    # create a container for votes as they are counted
    temp_sample = pd.DataFrame(columns=sample.columns)
    print("sampling data for analysis ... complete")
 
    # prepare datasets to contain elected and eliminated candidates
    print("preparing results containers ...", end="\r")
    elected = pd.DataFrame(columns=candidates.columns)
    eliminated = pd.DataFrame(columns=elected.columns)
    reached_quota = pd.DataFrame(columns=elected.columns)
    exhausted = 0.0
    fractions = 0.0
    results = pd.DataFrame(columns=["event", "total votes", "exhausted", "fractions"] + list(candidates.cname))
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
    # stop if five members have been elected    
    while len(elected) < 5:
        print(f"\nCOUNT No. {count_no}\n")        
        # check if candidates running out
        if 6 - len(elected) > len(candidates):
            print("all remaining candidates elected")
            for candidate in candidates.index:
                elect_candidate(candidate)
        else:
            # check if candidates elected
            for candidate in candidates.index:
                if candidates.at[candidate, "votes"] >= quota:
                    reached_quota.loc[candidate] = candidates.loc[candidate]
            if len(reached_quota) == 0:
                print("no candidates elected")
                # flush out transient votes
                temp_sample.drop(index=temp_sample.index, inplace=True)
                eliminated_candidate = eliminate_candidate()
                distribute_eliminated_votes(eliminated_candidate)
            else:
                # elect first candidate to reach quota
                candidate = reached_quota.index[0]
                elect_candidate(candidate)
                distribute_surplus_votes(candidate)
                reached_quota = reached_quota.iloc[1:]                
        candidates.sort_values("votes", ascending=False, inplace=True)
        count_no = count_no + 1

    # simulation results
    print()
    print(elected[["cname", "party", "primary"]])

# exit program
print("\nEnjoy your day.")