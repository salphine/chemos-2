import streamlit as st
import hashlib
import time
from database import Database

class Authentication:
    def __init__(self):
        self.db = Database()
        
    def hash_password(self, password):
        """Simple password hashing"""
        return hashlib.sha256(password.encode()).hexdigest()
    
    def login(self, username, password):
        """Authenticate user"""
        query = "SELECT * FROM users WHERE username = %s AND password = %s AND is_active = TRUE"
        hashed_password = self.hash_password(password)
        
        result = self.db.execute_query(query, (username, hashed_password))
        
        if result and len(result) > 0:
            user = result[0]
            return {
                'authenticated': True,
                'username': user['username'],
                'role': user['role'],
                'user_id': user['id']
            }
        
        # For demo purposes, check default credentials
        if username == 'admin' and password == 'admin123':
            return {
                'authenticated': True,
                'username': 'admin',
                'role': 'admin',
                'user_id': 1
            }
        
        return {'authenticated': False, 'error': 'Invalid credentials'}
    
    def logout(self):
        """Clear session state"""
        for key in list(st.session_state.keys()):
            del st.session_state[key]
    
    def check_auth(self):
        """Check if user is authenticated"""
        if 'authenticated' not in st.session_state:
            return False
        return st.session_state.authenticated