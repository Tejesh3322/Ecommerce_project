"""
Database Setup and Models using SQLAlchemy
For Customer Profile Sync - SCD Type 1 Implementation
"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# Database configuration - using SQLite for easy testing
DATABASE_URL = "sqlite:///customers.db"

# Create engine and session
engine = create_engine(DATABASE_URL, echo=False)
Session = sessionmaker(bind=engine)

# Base class for models
Base = declarative_base()


class Customer(Base):
    """
    Customer model representing the customers table.
    SCD Type 1: Only current values are stored, no history maintained.
    """
    __tablename__ = 'customers'

    customer_id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    email = Column(String(255), nullable=False)
    phone_number = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<Customer(id={self.customer_id}, name='{self.name}', email='{self.email}')>"

    def to_dict(self):
        """Convert customer to dictionary for comparison"""
        return {
            'customer_id': self.customer_id,
            'name': self.name,
            'email': self.email,
            'phone_number': self.phone_number
        }


def init_database():
    """Initialize the database by creating all tables"""
    Base.metadata.create_all(engine)
    print("Database initialized successfully!")


def get_session():
    """Get a new database session"""
    return Session()


if __name__ == "__main__":
    # Initialize database when run directly
    init_database()
