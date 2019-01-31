import commun
import pandas as pd

input_file = "/Users/eugenie.ly/Documents/La REM/données/données électorales/présidentielles/2017_T1/bureau de vote/présidentielles2017_T1_bdv_OpenDataSoft__election-presidentielle-2017-resultats-par-bureaux-de-vote-tour-1.csv"
output_file = '/Users/eugenie.ly/Documents/La REM/données/données électorales/présidentielles/2017_T1/bureau de vote/présidentielles2017_T1_bdv_pour_elasticsearch.csv'
separator = ';'

nom_election = "Présidentielle 2017-T1"
date_election = '2017-04-23'


res_electoral_insee = pd.read_csv(input_file, sep=separator,
                                 dtype={'Code du département': str, 'Département':str, 'Code de la circonscription':str,
                                        'Circonscription':str, 'Code de la commune':str, 'Commune':str, 'Bureau de vote':str, 'Code Insee':str,
                                        'Coordonnées':str, 'Nom Bureau Vote':str, 'Adresse':str, 'Code Postal':str, 'Ville':str,'uniq_bdv':str})

res_electoral_insee.rename(columns={'Code du département': 'code_insee_departement', 'Département': 'nom_departement', 'Code de la circonscription': 'code_circonscription',
       'Circonscription':'nom_circonscription', 'Code de la commune':'code_insee_commune_intermediaire', 'Commune':'nom_commune_bdv', 'Bureau de vote':'code_bdv',
       'Inscrits':'nb_inscrits', 'Abstentions':'nb_abstentions', '% Abs/Ins': 'abstentions_sur_inscrits_pc', 'Votants':'nb_votants', '% Vot/Ins':'votants_sur_inscrits_pc',
       'Blancs':'nb_blancs', '% Blancs/Ins':'blancs_sur_inscrits_pc', '% Blancs/Vot':'blancs_sur_votants_pc', 'Nuls': 'nb_nuls', '% Nuls/Ins':'nuls_sur_inscrits_pc',
       '% Nuls/Vot':'nuls_sur_votants_pc', 'Exprimés':'nb_exprimes', '% Exp/Ins':'exprimes_sur_inscrits_pc', '% Exp/Vot':'exprimes_sur_votants_pc', 'N°Panneau':'numero_panneau', 'Sexe':'sexe_candidat',
       'Nom':'nom_candidat', 'Prénom':'prenom_candidat', 'Voix':'nb_voix', '% Voix/Ins':'voix_sur_inscrits_pc', '% Voix/Exp':'voix_sur_exprimes_pc', 'Code Insee':'code_insee_commune',
       'Coordonnées':'latitude_longitude_bdv', 'Nom Bureau Vote':'nom_bdv', 'Adresse':'adresse_bdv', 'Code Postal':'code_postal', 'Ville':'ville_adresse_bdv',
       'uniq_bdv':'uniq_bdv'}, inplace=True)
res_electoral_insee.drop(columns=['code_insee_commune_intermediaire', 'numero_panneau', 'uniq_bdv'], inplace=True)
res_electoral_insee['nom_complet_candidat'] = res_electoral_insee['prenom_candidat'].str.title()+' '+res_electoral_insee['nom_candidat'].str.title()
L = ['code_insee_departement', 'nom_departement', 'code_circonscription',
 'nom_circonscription','nom_commune_bdv','sexe_candidat','nom_candidat', 'prenom_candidat',
 'nom_bdv','adresse_bdv','ville_adresse_bdv']

res_electoral_insee['code_insee_commune'] = res_electoral_insee['code_insee_commune'].apply(lambda x: commun.change_code_commune(commun.padding(x), FDE=1))
res_electoral_insee.nom_departement = res_electoral_insee.nom_departement.str.title()
res_electoral_insee.code_insee_departement = res_electoral_insee.code_insee_departement.apply(lambda x: commun.change_code_dep(x))

for c in L :
    res_electoral_insee[c] = res_electoral_insee[c].str.title()
res_electoral_insee['code_bdv'] = res_electoral_insee['code_insee_commune']+'_'+res_electoral_insee['code_bdv']

#Solution 1
#res1 = commun.merge_epci(res_electoral_insee)
#res2 = commun.traitement_paris(res1)
#res3 = commun.traitement_lyon(res2)
#res4 = commun.traitement_marseille(res3)
#res5 = commun.info_commune(res4, nom_election, date_election)
#res = jointure_code_commune(res5)

#Solution 2
commun.check_format(res_electoral_insee)
res1 = commun.traitement_election(res_electoral_insee, nom_election, date_election)
res = commun.jointure(res1)
res.to_csv(output_file, index=False)