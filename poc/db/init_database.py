from .session import engine, SessionLocal


def init_db():
    from .models import Base
    from .sample_data import seed
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as s:
        seed(s)

if __name__ == "__main__":
    init_db()
    print("✅ DB initialized with sample data")
