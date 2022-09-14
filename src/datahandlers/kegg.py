import requests
import traceback
from more_itertools import chunked
from src.prefixes import KEGGCOMPOUND

###
#   KEGG
###

# KEGG has a set of compounds that have a 'sequence' tag
# according to https://www.genome.jp/kegg/compound/:
#   Peptide entries in KEGG COMPOUND are designated with "Peptide"
#   in the first Entry line (see example here). They are always
#   represented as sequence information using the three-letter
#   amino acid codes, but they may or may not contain the full
#   atomic structure representation. Small bioactive peptides are
#   categorized in the BRITE hierarchy file shown below.
# Following the referenced link leads one to
# https://www.genome.jp/kegg-bin/download_htext?htext=br08005.keg&format=json&filedir=
# Which can be parsed to find the KEGG compounds that have a sequence.
# As for crawling them and pulling the sequence, should we be going through the KEGG client? probably?


def pull_kegg_compound_labels(outfile):
    try:

        request_session = requests.Session()
        request_adapter = requests.adapters.HTTPAdapter(max_retries=10)
        request_session.mount("http://", request_adapter)
        request_session.mount("https://", request_adapter)

        # 1:22530 + 1 to have inclusive end
        compound_chunks = list(chunked(range(1, 22531), 10))

        with open(outfile, "w") as lfile:
            for compound_chunk in compound_chunks:
                rid = "+".join([f"C{str(x).zfill(5)}" for x in compound_chunk])
                url = f"https://rest.kegg.jp/get/cpd:{rid}"
                raw_results = request_session.get(url)
                raw_lines = raw_results.text.split("\n")
                for idx, line in enumerate(raw_lines):
                    if not line.startswith("ENTRY"):
                        continue

                    kegg_id = line.split()[1:][0]

                    name_line = raw_lines[idx + 1]
                    if name_line.startswith("NAME"):
                        name = " ".join(name_line.split()[1:])
                        if name.endswith(";"):
                            name = name[:-1]

                        keggid = f"{KEGGCOMPOUND}:{kegg_id}"
                        lfile.write(f"{keggid}\t{name}\n")

    except BaseException as e:
        traceback.print_exception(e)
        raise e


# Not sure if we need this stuff atm...


def pull_br_file(br):
    r = requests.get(f"https://www.genome.jp/kegg-bin/download_htext?htext=br{br}.keg&format=json&filedir=")
    j = r.json()
    identifiersandnames = []
    handle_kegg_list(j["children"], identifiersandnames)
    return identifiersandnames


def pull_kegg_sequences():
    kegg_sequences = defaultdict(set)
    r = requests.get("https://www.genome.jp/kegg-bin/download_htext?htext=br08005.keg&format=json&filedir=")
    j = r.json()
    identifiersandnames = []
    handle_kegg_list(j["children"], identifiersandnames)
    identifiers = [x[0] for x in identifiersandnames]
    for i, kid in enumerate(identifiers):
        s = get_sequence(kid)
        kegg_sequences[s].add(f"KEGG.COMPOUND:{kid}")
    return kegg_sequences


def handle_kegg_list(childlist, names):
    for child in childlist:
        if "children" in child:
            handle_kegg_list(child["children"], names)
        else:
            n = child["name"].split()
            names.append((n[0], " ".join(n[1:])))


def get_sequence(compound_id):
    onetothree = {
        "A": "Ala",
        "B": "Asx",
        "C": "Cys",
        "D": "Asp",
        "E": "Glu",
        "F": "Phe",
        "G": "Gly",
        "H": "His",
        "I": "Ile",
        "K": "Lys",
        "L": "Leu",
        "M": "Met",
        "N": "Asn",
        "P": "Pro",
        "Q": "Gln",
        "R": "Arg",
        "S": "Ser",
        "T": "Thr",
        "V": "Val",
        "W": "Trp",
        "X": "X",
        "Y": "Tyr",
        "Z": "Glx",
    }
    aamap = {v: k for k, v in onetothree.items()}
    # phosphoGlutamate?  This matches for
    aamap["Glp"] = "Q"
    url = f"http://rest.kegg.jp/get/cpd:{compound_id}"
    raw_results = requests.get(url)  # .json()
    results = raw_results.text.split("\n")
    mode = "looking"
    x = ""
    for line in results:
        if mode == "looking" and line.startswith("SEQUENCE"):
            x = " ".join(line.strip().split()[1:])
            mode = "reading"
        elif mode == "reading":
            ls = line.strip()
            if ls.startswith("ORGANISM") or ls.startswith("TYPE"):
                break
            x += " " + ls
    # At least one of these things contains a one-letter AA sequence (?!) C16008.  Try to recognize it
    toks = x.split()
    lens = [len(t) for t in toks]
    modelen = max(set(lens), key=lens.count)
    if modelen == 10:
        # single aa code, broken into blocks of 10
        return "".join(toks)
    elif modelen != 3:
        # probably still a one-AA list, but let's check some cases
        if len(toks) == 1 or len(toks[0]) == 10:
            return "".join(toks)
        else:
            print("not sure what this is", x)
            raise (x)
    # OK, anything left should be a 3-letter AA string
    # remove parenthetical comments
    regex = "\((.*?)\)"
    xprime = re.sub(regex, "", x)
    # do a cleanup for things like Arg-NH2
    xps = xprime.split()
    c = []
    for a in xprime.split():
        q = a.split("-")
        for qq in q:
            if qq in aamap:
                c.append(qq)
                break
    # Change to 1 letter codes
    s = [aamap[a] for a in c]
    return "".join(s)


if __name__ == "__main__":
    # pull_uniprot(repull=True)
    keggs = pull_kegg_compounds()
