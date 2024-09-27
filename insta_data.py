# %%
import instaloader
import csv
from datetime import datetime


def extract_instagram_data(username, output_file):
    # Créer une instance de Instaloader
    L = instaloader.Instaloader()

    try:
        # Charger le profil
        profile = instaloader.Profile.from_username(L.context, username)

        # Ouvrir le fichier CSV en mode écriture
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Écrire l'en-tête
            writer.writerow(['Date', 'Likes', 'Comments', 'Caption', 'Hashtags'])

            # Parcourir les posts et écrire les données
            for post in profile.get_posts():
                date = post.date.strftime("%Y-%m-%d %H:%M:%S")
                likes = post.likes
                comments = post.comments
                caption = post.caption if post.caption else ""
                hashtags = ", ".join(post.caption_hashtags)

                writer.writerow([date, likes, comments, caption, hashtags])

        print(f"Données extraites et enregistrées dans {output_file}")

    except instaloader.exceptions.ProfileNotExistsException:
        print(f"Le profil {username} n'existe pas.")
    except Exception as e:
        print(f"Une erreur s'est produite : {str(e)}")

# Utilisation de la fonction
username = "m_usicheals"
output_file = "instagram_data.csv"
extract_instagram_data(username, output_file)
# %%
