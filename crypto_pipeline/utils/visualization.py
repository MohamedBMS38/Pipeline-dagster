"""
Utilitaires pour la visualisation des données de cryptomonnaies.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import List, Dict, Any, Optional
from datetime import datetime

def ensure_visualization_dir(base_dir: str = "crypto_pipeline/data") -> str:
    """
    S'assure que le répertoire de visualisation existe.
    
    Args:
        base_dir: Répertoire de base pour les visualisations.
        
    Returns:
        Chemin du répertoire de visualisation.
    """
    vis_dir = os.path.join(base_dir, "visualizations")
    os.makedirs(vis_dir, exist_ok=True)
    return vis_dir

def create_price_chart(
    price_data: pd.DataFrame,
    coin_name: str,
    output_path: Optional[str] = None,
    days: int = 30,
    show_volume: bool = True
) -> str:
    """
    Crée un graphique d'évolution des prix pour une cryptomonnaie.
    
    Args:
        price_data: DataFrame contenant les données de prix (doit contenir 'timestamp' et 'price').
        coin_name: Nom de la cryptomonnaie.
        output_path: Chemin de sortie pour le graphique (facultatif).
        days: Nombre de jours à afficher dans le titre.
        show_volume: Indique si le volume doit être affiché.
        
    Returns:
        Chemin du fichier graphique créé.
    """
    # Créer une figure avec deux sous-graphiques si l'affichage du volume est demandé
    if show_volume and 'total_volume' in price_data.columns:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), gridspec_kw={'height_ratios': [3, 1]})
    else:
        fig, ax1 = plt.subplots(figsize=(12, 6))
    
    # Formater l'axe des dates
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    ax1.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    
    # Tracer le graphique de prix
    ax1.plot(price_data['timestamp'], price_data['price'], 'b-', linewidth=2)
    ax1.set_title(f"Évolution du prix de {coin_name} sur {days} jours", fontsize=16)
    ax1.set_ylabel('Prix (USD)', fontsize=12)
    ax1.grid(True, linestyle='--', alpha=0.7)
    
    # Tracer le volume si demandé
    if show_volume and 'total_volume' in price_data.columns and 'ax2' in locals():
        ax2.bar(price_data['timestamp'], price_data['total_volume'], color='gray', alpha=0.5)
        ax2.set_ylabel('Volume', fontsize=12)
        ax2.grid(True, linestyle='--', alpha=0.7)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
        ax2.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Déterminer le chemin de sortie
    if output_path is None:
        vis_dir = ensure_visualization_dir()
        filename = f"price_chart_{coin_name.lower().replace(' ', '_')}_{datetime.now().strftime('%Y%m%d')}.png"
        output_path = os.path.join(vis_dir, filename)
    
    # Sauvegarder le graphique
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    return output_path

def create_comparison_chart(
    coins_data: Dict[str, pd.DataFrame],
    metric: str = 'price',
    title: str = "Comparaison des cryptomonnaies",
    output_path: Optional[str] = None,
    normalize: bool = True
) -> str:
    """
    Crée un graphique comparatif pour plusieurs cryptomonnaies.
    
    Args:
        coins_data: Dictionnaire contenant les DataFrames de données pour chaque cryptomonnaie.
        metric: Métrique à comparer ('price', 'market_cap', etc.)
        title: Titre du graphique.
        output_path: Chemin de sortie pour le graphique (facultatif).
        normalize: Indique si les valeurs doivent être normalisées pour une meilleure comparaison.
        
    Returns:
        Chemin du fichier graphique créé.
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    for coin_name, df in coins_data.items():
        if metric not in df.columns:
            continue
            
        # Normaliser les valeurs si demandé
        if normalize and len(df) > 0:
            first_value = df[metric].iloc[0]
            values = df[metric] / first_value * 100 if first_value != 0 else df[metric]
            ax.plot(df['timestamp'], values, linewidth=2, label=coin_name)
        else:
            ax.plot(df['timestamp'], df[metric], linewidth=2, label=coin_name)
    
    # Formater le graphique
    if normalize:
        ax.set_ylabel('Valeur relative (%)', fontsize=12)
        title += " (normalisé)"
    else:
        ax.set_ylabel(metric.capitalize(), fontsize=12)
    
    ax.set_title(title, fontsize=16)
    ax.grid(True, linestyle='--', alpha=0.7)
    ax.legend(loc='best', fontsize=10)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m/%Y'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    
    plt.xticks(rotation=45)
    plt.tight_layout()
    
    # Déterminer le chemin de sortie
    if output_path is None:
        vis_dir = ensure_visualization_dir()
        filename = f"comparison_{metric}_{datetime.now().strftime('%Y%m%d')}.png"
        output_path = os.path.join(vis_dir, filename)
    
    # Sauvegarder le graphique
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    return output_path

def create_market_overview(
    market_data: pd.DataFrame,
    output_path: Optional[str] = None,
    top_n: int = 10
) -> str:
    """
    Crée un aperçu du marché des cryptomonnaies sous forme de visualisation.
    
    Args:
        market_data: DataFrame contenant les données de marché.
        output_path: Chemin de sortie pour le graphique (facultatif).
        top_n: Nombre de cryptomonnaies à inclure dans l'aperçu.
        
    Returns:
        Chemin du fichier graphique créé.
    """
    # Filtrer les N principales cryptomonnaies par capitalisation boursière
    top_coins = market_data.sort_values('market_cap', ascending=False).head(top_n)
    
    # Créer une figure avec des sous-graphiques
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 8))
    
    # Graphique 1: Capitalisation boursière
    ax1.barh(top_coins['name'], top_coins['market_cap'] / 1e9)
    ax1.set_title("Top Capitalisation Boursière (Milliards USD)")
    ax1.set_xlabel("Capitalisation (Milliards USD)")
    ax1.invert_yaxis()  # Pour que le plus grand soit en haut
    
    # Graphique 2: Variation de prix sur 24h
    colors = ['g' if x >= 0 else 'r' for x in top_coins['price_change_percentage_24h']]
    ax2.barh(top_coins['name'], top_coins['price_change_percentage_24h'], color=colors)
    ax2.set_title("Variation de Prix sur 24h (%)")
    ax2.set_xlabel("Variation (%)")
    ax2.invert_yaxis()  # Pour maintenir le même ordre que le premier graphique
    
    plt.tight_layout()
    
    # Déterminer le chemin de sortie
    if output_path is None:
        vis_dir = ensure_visualization_dir()
        filename = f"market_overview_{datetime.now().strftime('%Y%m%d')}.png"
        output_path = os.path.join(vis_dir, filename)
    
    # Sauvegarder le graphique
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close(fig)
    
    return output_path 