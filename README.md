# TOOLBOX QUINTEN

Librairie contenant l'ensemble des modules développés par Quinten.

Ce projet s'organise en trois catégories :

    • Cleaning
    • Dataprep (pyspark)
    • Machine Learning

## Installation

### Linux :

Dans un terminal :

```sh
git config --global http.sslVerify false
git clone https://master.quinten.local/gitlab/quinten/toolbox.git
cd toolbox
python setup.py install 
```
Puis ajouter le chemin vers le dossier toolbox au PYTHONPATH.

### Windows :

Installer git depuis l'executable qui se trouve sur le partage :
Z:\Outils_et_logos\3-Logiciels\Versionning\Git-1.9.4-preview20140611.exe

Avec l'explorateur de fichier windows faire un click droit sur le dossier qui contiendra la librairie et cliquer sur "Git Bash Here"

Un terminal se lance, se reporter aux commandes de la partie Linux.


# Lexique

• Variable(s) identifiant(s) = variable(s) servant à identifier la maille de la table finale

• Variable(s) de découpage   = variable(s) servant à identifier la/les modalités permettant de définir les sous-groupes sur lesquelles les calculs vont être effectués (variables à « dummifier »)

• Variable(s) de calculs     = variable(s) sur lesquelles vont s’effectuer les calculs


   

