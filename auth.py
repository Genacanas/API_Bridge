from datetime import datetime, timedelta
import os
from typing import Optional

import jwt
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from passlib.context import CryptContext
from pydantic import BaseModel
import pyodbc

from database import get_auth_db

# Security Settings
SECRET_KEY = os.environ.get("JWT_SECRET_KEY", "your-super-secret-key-change-in-prod")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60 * 24 # 24 hours

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/login")

class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None

class UserInDB(User):
    hashed_password: str

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def create_users_table_if_not_exists(db: pyodbc.Connection):
    """Creates the users table in the auth database if it doesn't exist."""
    cursor = db.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='users' AND xtype='U')
        BEGIN
            CREATE TABLE users (
                id INT IDENTITY(1,1) PRIMARY KEY,
                username VARCHAR(255) UNIQUE NOT NULL,
                hashed_password VARCHAR(255) NOT NULL,
                email VARCHAR(255) NULL,
                full_name VARCHAR(255) NULL,
                disabled BIT DEFAULT 0,
                created_at DATETIME DEFAULT GETUTCDATE()
            )
        END
    """)
    db.commit()

def create_initial_admin(db: pyodbc.Connection):
    """Creates an initial admin user (AdminRokas / o$RRy6aocnlY&R) if the users table is empty."""
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM users")
    count = cursor.fetchone()[0]
    if count == 0:
        hashed_pw = get_password_hash("o$RRy6aocnlY&R")
        cursor.execute(
            "INSERT INTO users (username, hashed_password, full_name) VALUES (?, ?, ?)",
            "AdminRokas", hashed_pw, "Administrator"
        )
        db.commit()
        print("Created default admin user: AdminRokas / o$RRy6aocnlY&R")

def get_user(db: pyodbc.Connection, username: str) -> Optional[UserInDB]:
    cursor = db.cursor()
    cursor.execute("SELECT username, email, full_name, disabled, hashed_password FROM users WHERE username = ?", username)
    row = cursor.fetchone()
    if row:
        return UserInDB(
            username=row.username,
            email=row.email,
            full_name=row.full_name,
            disabled=row.disabled,
            hashed_password=row.hashed_password
        )
    return None

async def get_current_user(token: str = Depends(oauth2_scheme), db: pyodbc.Connection = Depends(get_auth_db)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except jwt.PyJWTError:
        raise credentials_exception
        
    user = get_user(db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user

async def get_current_active_user(current_user: User = Depends(get_current_user)):
    if current_user.disabled:
        raise HTTPException(status_code=400, detail="Inactive user")
    return current_user
