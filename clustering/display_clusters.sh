xsv search -s valid_cluster $1 final-clustering.csv | xsv sort -s $1 | xsv select name,birth_B,death_B,final_occupation_L2_B,final_citizenship | xsv table
