import commun
import pandas as pd

input_file = "/Users/eugenie.ly/Documents/La REM/données/données électorales/européennes/2014/bureau de vote/européennes2014_bdv_OpenDataSoft__resultats-des-elections-europeennes-2014.csv"
output_file = '/Users/eugenie.ly/Documents/La REM/données/données électorales/présidentielles/2017_T1/bureau de vote/européennes2014_bdv_pour_elasticsearch.csv'
separator = ';'

nom_election = "Européenne 2014"
date_election = '2014-05-24'

res_2014 = pd.read_csv(input_file, sep=';',
                       dtype={'No tour': str, 'Code departement': str, 'Code de la commune': str,
                              'Nom de la commune': str, 'No de bureau de vote': str, 'Inscrits': str, 'Votants': str,
                              'Exprimes': str, 'No de depot du candidat': str, 'Nom du candidat': str,
                              'Prenom du candidat': str, 'Code nuance du candidat': str,
                              'Nombre de voix du candidat': str, 'Code Insee': str, 'Coordonnees': str,
                              'Candidat': str})

res_2014.rename(columns={'Code departement': 'code_insee_departement', 'Nom de la commune': 'nom_commune_bdv',
                         'No de bureau de vote': 'code_bdv_intermédiaire', 'Inscrits': 'nb_inscrits',
                         'Votants': 'nb_votants', 'Exprimes': 'nb_exprimes', 'Nom du candidat': 'nom_candidat',
                         'Prenom du candidat': 'prenom_candidat', 'Code nuance du candidat': 'code_étiquette_politique',
                         'Nombre de voix du candidat': 'nb_voix', 'Code Insee': 'code_insee_commune',
                         'Candidat': 'nom_complet_candidat'}, inplace=True)
res_2014.drop(columns=['No tour', 'No de depot du candidat', 'Code de la commune', 'Coordonnees'], inplace=True)

res_2014.code_insee_departement = res_2014.code_insee_departement.apply(lambda x: commun.change_code_dep(commun.padding(x, 2)))
res_2014['code_insee_commune'] = res_2014['code_insee_commune'].apply(lambda x: commun.code_insee_6_car(commun.padding(x)))

L = ['code_insee_departement', 'nom_commune_bdv', 'nom_candidat', 'prenom_candidat', 'nom_complet_candidat']
for c in L:
    res_2014[c] = res_2014[c].str.title()

#res_2014 = pd.merge(res_2014, dep[['code_insee_departement', 'nom_departement']], on='code_insee_departement', how='left')

res_2014['nb_abstentions'] = res_2014['nb_inscrits'].astype(int) - res_2014['nb_exprimes'].astype(int)

res_2014['abstentions_sur_inscrits_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_abstentions', 'nb_inscrits'), axis=1)
res_2014['abstentions_sur_votants_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_abstentions', 'nb_votants'), axis=1)
res_2014['exprimes_sur_inscrits_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_exprimes', 'nb_inscrits'), axis=1)
res_2014['exprimes_sur_votants_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_exprimes', 'nb_votants'), axis=1)
res_2014['votants_sur_inscrits_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_votants', 'nb_inscrits'), axis=1)
res_2014['voix_sur_inscrits_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_voix', 'nb_inscrits'), axis=1)
res_2014['voix_sur_exprimes_pc'] = res_2014.apply(lambda x: commun.pc(x, 'nb_voix', 'nb_exprimes'), axis=1)

res_2014['code_bdv'] = res_2014['code_insee_commune'] + '_' + res_2014['code_bdv_intermédiaire']

res_2014.drop(columns=['code_bdv_intermédiaire'], inplace=True)

col = ['nom_circonscription',
       'code_bdv',
       'nom_bdv',
       'adresse_bdv',
       'code_postal',
       'ville_adresse_bdv',
       'latitude_longitude_bdv']

#res_2014 = pd.merge(res_2014, res_2017[col], on='code_bdv', how='left')

commun.check_format(res_2014)
res1 = commun.traitement_election(res_2014, nom_election, date_election)
res = commun.jointure(res1, col)
res.to_csv(output_file, index=False)











