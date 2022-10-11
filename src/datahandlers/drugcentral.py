from src.prefixes import DRUGCENTRAL
import psycopg2

def pull_drugcentral(structfile, labelfile, xreffile):
    #DrugCentral is only available as a postgres db, but fortunately they run a public instance.
    conn = psycopg2.connect("host=unmtid-dbs.net dbname=drugcentral user=drugman port=5433 password=dosage")
    cur = conn.cursor()
    cur.execute("SELECT id, identifier, id_type, struct_id, parent_match FROM identifier;")
    x = cur.fetchall()
    #Pull xrefs.  Output format is "1396790	C1172734	UMLSCUI	4970	None"
    # meaning UMLSCUI:C1172734 == DrugBank:4970
    with open(xreffile,'w') as outf:
        for xi in x:
            xs = [str(y) for y in xi]

            row_id = xs[0]
            identifier = xs[1]
            id_type = xs[2]
            struct_id = xs[3]
            parent_match = xs[4]

            # Sometimes (always?) CHEBI identifiers have a CHEBI: prefix. If this occurs, we remove the prefix before
            # writing it out to this file.
            if id_type == 'CHEBI' and identifier.startswith('CHEBI:'):
                identifier = identifier[6:]

            outf.write(f'{row_id}\t{identifier}\t{id_type}\t{struct_id}\t{parent_match}\n')
    cur.execute("SELECT id, smiles, name FROM structures;")
    x = cur.fetchall()
    with open(labelfile, 'w') as outf, open(structfile, 'w') as sf:
        for xi in x:
            outf.write(f'{DRUGCENTRAL}:{xi[0]}\t{xi[2]}\n')
            sf.write(f'{DRUGCENTRAL}:{xi[0]}\t{xi[1]}\n')
