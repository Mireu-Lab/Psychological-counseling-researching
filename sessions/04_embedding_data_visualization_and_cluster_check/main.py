from __future__ import annotations

from pathlib import Path

import hdbscan
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import polars as pl
import umap
from google.cloud import bigquery
from google.oauth2 import service_account
from sklearn.feature_extraction.text import TfidfVectorizer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
KEY_PATH = PROJECT_ROOT / ".key/testprojects-453622-d1f78fcce8b7.json"
FONT_PATH = PROJECT_ROOT / ".fonts/NotoSansKR-Bold.ttf"
PROJECT_ID = "testprojects-453622"
DATASET_ID = "Psychological_counseling_data"
SOURCE_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.morpheme_classification`"


def create_client() -> bigquery.Client:
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)


def prepare_font() -> str:
    if not FONT_PATH.exists():
        raise FileNotFoundError(f"Font file not found: {FONT_PATH}")
    fm.fontManager.addfont(str(FONT_PATH))
    return fm.FontProperties(fname=str(FONT_PATH)).get_name()


def main() -> None:
    if not KEY_PATH.exists():
        raise FileNotFoundError(f"GCP key not found: {KEY_PATH}")

    client = create_client()
    print(f"Client created with project: {client.project}")

    query = """
        SELECT split_contents
        FROM `testprojects-453622.Psychological_counseling_data.morpheme_classification`
        WHERE split_contents IS NOT NULL
          AND session_no = 1
    """
    df = client.query(query).to_dataframe()
    counseling_data = df["split_contents"].dropna().astype(str).tolist()
    print(f"가져온 데이터 수: {len(counseling_data)}")

    print("2. 표현 형태(말투/구조) 기반 벡터화를 진행 중입니다...")
    vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(2, 4), max_features=10000)
    embeddings = vectorizer.fit_transform(counseling_data).toarray()

    print("3. UMAP으로 차원 축소를 진행 중입니다...")
    umap_model = umap.UMAP(n_neighbors=15, n_components=2, metric="cosine", random_state=42)
    umap_embeddings = umap_model.fit_transform(embeddings)

    print("4. HDBSCAN으로 클러스터링을 진행 중입니다...")
    clusterer = hdbscan.HDBSCAN(min_cluster_size=10, metric="euclidean", cluster_selection_method="eom")
    clusters = clusterer.fit_predict(umap_embeddings)

    df["Cluster_ID"] = clusters
    print("\n--- 클러스터링 결과 샘플 ---")
    print(df.head(10))

    print("5. 시각화 생성 중...")
    font_name = prepare_font()
    plt.rcParams["font.family"] = font_name
    plt.rcParams["axes.unicode_minus"] = False

    plt.figure(figsize=(10, 8))
    scatter = plt.scatter(
        umap_embeddings[:, 0],
        umap_embeddings[:, 1],
        c=clusters,
        cmap="Spectral",
        s=10,
        alpha=0.5,
    )
    plt.title("BigQuery 기반 상담 표현 형태 클러스터링 (UMAP + HDBSCAN)")
    plt.xlabel("UMAP Dimension 1")
    plt.ylabel("UMAP Dimension 2")
    plt.colorbar(scatter, label="Cluster ID")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()