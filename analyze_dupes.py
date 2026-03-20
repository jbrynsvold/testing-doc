import csv
from collections import Counter

email_seen = Counter()
supp_seen = Counter()
name_co_seen = Counter()

with open("your_file.csv", newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    for row in reader:
        email = (row.get("Email Address") or "").strip().lower()
        supp  = (row.get("Supplemental Email") or "").strip().lower()
        first = (row.get("First Name") or "").strip().lower()
        last  = (row.get("Last Name") or "").strip().lower()
        co    = (row.get("Company Name") or "").strip().lower()

        if email:
            email_seen[email] += 1
        if supp:
            supp_seen[supp] += 1
        if first and last and co:
            name_co_seen[(first, last, co)] += 1

email_dupes    = sum(1 for v in email_seen.values() if v > 1)
supp_dupes     = sum(1 for v in supp_seen.values() if v > 1)
name_co_dupes  = sum(1 for v in name_co_seen.values() if v > 1)

print(f"Duplicate emails in file:          {email_dupes:,}")
print(f"Duplicate supplemental emails:     {supp_dupes:,}")
print(f"Duplicate name+company combos:     {name_co_dupes:,}")
print(f"Total unique emails:               {len(email_seen):,}")
print(f"Total unique supp emails:          {len(supp_seen):,}")
print(f"Total unique name+company combos:  {len(name_co_seen):,}")
