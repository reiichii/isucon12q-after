from dataclasses import dataclass
import os
from typing import Optional
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from sqltrace import initialize_sql_logger

admin_db: Engine = None


def connect_admin_db() -> Engine:
    """管理用DBに接続する"""
    host = os.getenv("ISUCON_DB_HOST", "127.0.0.1")
    port = os.getenv("ISUCON_DB_PORT", 3306)
    user = os.getenv("ISUCON_DB_USER", "isucon")
    password = os.getenv("ISUCON_DB_PASSWORD", "isucon")
    database = os.getenv("ISUCON_DB_NAME", "isuports")

    return create_engine(
        f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}",
        pool_size=10,
    )


admin_db = connect_admin_db()


def tenant_db_path(id: int) -> str:
    """テナントDBのパスを返す"""
    tenant_db_dir = os.getenv("ISUCON_TENANT_DB_DIR", "../tenant_db")
    return tenant_db_dir + f"/{id}.db"


def connect_to_tenant_db(id: int) -> Engine:
    """テナントDBに接続する"""
    path = tenant_db_path(id)
    engine = create_engine(f"sqlite:///{path}")
    return initialize_sql_logger(engine)


@dataclass
class CompetitionRow:
    tenant_id: int
    id: str
    title: str
    finished_at: Optional[int]
    created_at: int
    updated_at: int


def retrieve_competition(tenant_db: Engine, id: str) -> Optional[CompetitionRow]:
    """大会を取得する"""
    row = tenant_db.execute("SELECT * FROM competition WHERE id = ?", id).fetchone()
    if not row:
        return None

    return CompetitionRow(**row)


def billing_report_by_competition(
    tenant_db: Engine, tenant_id: int, competition_id: str
):
    """大会ごとの課金レポートを計算する"""
    competition = retrieve_competition(tenant_db, competition_id)
    if not competition:
        raise RuntimeError("error retrieveCompetition")

    visit_history_summary_rows = admin_db.execute(
        "SELECT player_id, MIN(created_at) AS min_created_at FROM visit_history WHERE tenant_id = %s AND competition_id = %s GROUP BY player_id",
        tenant_id,
        competition.id,
    ).fetchall()

    billing_map = {}
    for vh in visit_history_summary_rows:
        # competition.finished_atよりもあとの場合は、終了後に訪問したとみなして大会開催内アクセス済みとみなさない
        if (
            bool(competition.finished_at)
            and competition.finished_at < vh.min_created_at
        ):
            continue
        billing_map[str(vh.player_id)] = "visitor"

    # スコアを登録した参加者のIDを取得する
    scored_player_id_rows = tenant_db.execute(
        "SELECT DISTINCT(player_id) FROM player_score WHERE tenant_id = ? AND competition_id = ?",
        tenant_id,
        competition.id,
    ).fetchall()

    for pid in scored_player_id_rows:
        # スコアが登録されている参加者
        billing_map[str(pid.player_id)] = "player"

    player_count = 0
    visitor_count = 0
    if bool(competition.finished_at):
        for category in billing_map.values():
            if category == "player":
                player_count += 1
            if category == "visitor":
                visitor_count += 1

    admin_db.execute(
        "INSERT INTO billing_report (tenant_id, competition_id, competition_title, player_count, visitor_count, billing_player_yen, billing_visitor_yen, billing_yen) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        tenant_id,
        competition.id,
        competition.title,
        player_count,
        visitor_count,
        100 * player_count,
        10 * visitor_count,
        100 * player_count + 10 * visitor_count,
    )


def main():
    # tenant一覧を取得
    tenant_rows = admin_db.execute("SELECT id FROM tenant").fetchall()
    for tenant_row in tenant_rows:
        # tenantごとに終了した大会を取得
        tenant_db = connect_to_tenant_db(int(tenant_row.id))
        competitions = tenant_db.execute(
            "SELECT * FROM competition WHERE tenant_id=? AND finished_at is not NULL",
            tenant_row.id,
        ).fetchall()
        for c in competitions:
            billing_report_by_competition(tenant_db, tenant_row.id, c.id)


if __name__ == "__main__":
    main()
