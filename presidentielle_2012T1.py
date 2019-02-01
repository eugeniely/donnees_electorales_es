import commun
import pandas as pd

input_file = "/Users/eugenie.ly/Documents/La REM/données/données électorales/présidentielles/2012_T1/bureau de vote/présidentielles2012_T1_bdv_OpenDataSoft__resultats-de-lelection-presidentielle-2012.csv"
output_file = '/Users/eugenie.ly/Documents/La REM/données/données électorales/présidentielles/2012_T1/bureau de vote/présidentielles2012_T1_bdv_pour_elasticsearch.csv'
separator = ';'

nom_election = "Présidentielle 2012-T1"
date_election = '2012-04-21'

res_2012 = pd.read_csv(input_file, sep=separator,
                       dtype={'No Tour': str, 'Departement': str, 'Code Commune': str, 'Commune': str,
                              'No de circonscription Lg': str, 'No de canton': str, 'No de bureau de vote': str,
                              'Inscrits': str, 'Votants': str, 'Exprimes': str, 'No de depot du candidat': str,
                              'Nom du candidat': str, 'Prenom du candidat': str, 'Code candidat': str,
                              'Nombre de voix du candidat': str, 'Code Insee': str, 'Coordonnees': str,
                              'Candidat': str})
res_2012 = res_2012[res_2012['No Tour'] == '1']
res_2012.rename(columns={'Departement': 'code_insee_departement', 'Commune': 'nom_commune_bdv',
                         'No de circonscription Lg': 'code_circonscription',
                         'No de bureau de vote': 'code_bdv_intermédiaire',
                         'Inscrits': 'nb_inscrits', 'Votants': 'nb_votants', 'Exprimes': 'nb_exprimes',
                         'Nom du candidat': 'nom_candidat', 'Prenom du candidat': 'prenom_candidat',
                         'Nombre de voix du candidat': 'nb_voix', 'Code Insee': 'code_insee_commune',
                         'Candidat': 'nom_complet_candidat'}, inplace=True)
res_2012.drop(
    columns=['No de canton', 'No Tour', 'Code Commune', 'Code candidat', 'No de depot du candidat', 'Coordonnees'],
    inplace=True)
res_2012.code_insee_departement = res_2012.code_insee_departement.apply(lambda x: commun.change_code_dep(commun.padding(x, 2)))
res_2012['code_insee_commune'] = res_2012['code_insee_commune'].apply(lambda x: commun.change_code_commune(commun.padding(x), FDE=1))

L = ['code_insee_departement', 'code_circonscription',
     'nom_commune_bdv', 'nom_candidat', 'prenom_candidat', 'nom_complet_candidat']
for c in L:
    res_2012[c] = res_2012[c].str.title()

res_2012['nb_abstentions'] = res_2012['nb_inscrits'].astype(int) - res_2012['nb_exprimes'].astype(int)
res_2012['abstentions_sur_inscrits_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_abstentions', 'nb_inscrits'), axis=1)
res_2012['abstentions_sur_votants_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_abstentions', 'nb_votants'), axis=1)
res_2012['exprimes_sur_inscrits_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_exprimes', 'nb_inscrits'), axis=1)
res_2012['exprimes_sur_votants_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_exprimes', 'nb_votants'), axis=1)
res_2012['votants_sur_inscrits_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_votants', 'nb_inscrits'), axis=1)
res_2012['voix_sur_inscrits_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_voix', 'nb_inscrits'), axis=1)
res_2012['voix_sur_exprimes_pc'] = res_2012.apply(lambda x: commun.pc(x, 'nb_voix', 'nb_exprimes'), axis=1)

def sexe_candidat(x):
    if x == 'Nicolas Dupont-Aignan':
        return 'M'
    if x == 'François Hollande':
        return 'M'
    if x == 'Nicolas Sarkozy':
        return 'M'
    if x == 'Jean-Luc Mélenchon':
        return 'M'
    if x == 'Philippe Poutou':
        return 'M'
    if x == 'Nathalie Arthaud':
        return 'F'
    if x == 'Jacques Cheminade':
        return 'M'
    if x == 'François Bayrou':
        return 'M'
    if x == 'Eva Joly':
        return 'F'
    if x == 'Marine Le Pen':
        return 'F'

res_2012['sexe_candidat'] = res_2012['nom_complet_candidat'].apply(lambda x: sexe_candidat(x))
res_2012['code_bdv'] = res_2012['code_insee_commune'] + '_' + res_2012['code_bdv_intermédiaire']

col = ['nom_circonscription',
       'code_bdv',
       'nom_bdv',
       'adresse_bdv',
       'code_postal',
       'ville_adresse_bdv',
       'latitude_longitude_bdv']

commun.check_format(res_2012)
res1 = commun.traitement_election(res_2012, nom_election, date_election)
res = commun.jointure(res1, col)
res.to_csv(output_file, index=False)