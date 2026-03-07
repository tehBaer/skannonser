"""
Manual overrides manager for property data.
Handles override operations for correcting property data (adresse/postnummer/pris).
"""
import sqlite3
from typing import Optional, Tuple, List
import os


class PropertyOverrides:
    """Manages manual overrides for property data."""
    
    def __init__(self, db_path: str = None):
        """Initialize with database path."""
        if db_path is None:
            # Default to database folder
            script_dir = os.path.dirname(os.path.abspath(__file__))
            db_path = os.path.join(script_dir, 'properties.db')
        
        self.db_path = db_path
    
    def _get_connection(self):
        """Get database connection."""
        return sqlite3.connect(self.db_path)
    
    def set_override(
        self,
        finnkode: str,
        pris: int = None,
        reason: str = None,
        adresse: str = None,
        postnummer: str = None,
    ):
        """
        Set manual override for a property's fields.
        
        Args:
            finnkode: The property's finn.no code
            pris: Override value for price
            adresse: Override address text
            postnummer: Override postal code
            reason: Explanation for the override
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        # Check if override already exists
        cursor.execute('SELECT id FROM manual_overrides WHERE finnkode = ?', (finnkode,))
        existing = cursor.fetchone()
        
        if existing:
            # Update existing override
            cursor.execute('''
                UPDATE manual_overrides
                SET pris = COALESCE(?, pris),
                    adresse = COALESCE(?, adresse),
                    postnummer = COALESCE(?, postnummer),
                    override_reason = COALESCE(?, override_reason),
                    updated_at = CURRENT_TIMESTAMP
                WHERE finnkode = ?
            ''', (pris, adresse, postnummer, reason, finnkode))
        else:
            # Insert new override
            cursor.execute('''
                INSERT INTO manual_overrides (finnkode, pris, adresse, postnummer, override_reason)
                VALUES (?, ?, ?, ?, ?)
            ''', (finnkode, pris, adresse, postnummer, reason))
        
        conn.commit()
        conn.close()
        
        print(
            f"✓ Override set for {finnkode}: "
            f"pris={pris}, adresse={adresse}, postnummer={postnummer}, reason={reason}"
        )
    
    def get_override(self, finnkode: str) -> Optional[Tuple[Optional[int], Optional[str], Optional[str], Optional[str]]]:
        """
        Get override values for a property if they exist.
        
        Args:
            finnkode: The property's finn.no code
            
        Returns:
            Tuple of (pris, adresse, postnummer, reason) or None if no override exists
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute(
            'SELECT pris, adresse, postnummer, override_reason FROM manual_overrides WHERE finnkode = ?',
            (finnkode,),
        )
        result = cursor.fetchone()
        conn.close()
        
        return result
    
    def list_overrides(self) -> List[Tuple]:
        """
        List all active overrides.
        
        Returns:
            List of tuples: (finnkode, pris, adresse, postnummer, reason, updated_at)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT finnkode, pris, adresse, postnummer, override_reason, updated_at
            FROM manual_overrides
            ORDER BY updated_at DESC
        ''')
        
        results = cursor.fetchall()
        conn.close()
        
        if not results:
            print("No overrides set")
            return []
        
        print("\n📋 Manual Overrides:")
        for finnkode, pris, adresse, postnummer, reason, updated_at in results:
            print(f"  {finnkode}:")
            if adresse:
                print(f"    - ADRESSE: {adresse}")
            if postnummer:
                print(f"    - Postnummer: {postnummer}")
            if pris is not None:
                print(f"    - PRIS: {pris}")
            if reason:
                print(f"    - Reason: {reason}")
            print(f"    - Updated: {updated_at}")
        
        return results
    
    def remove_override(self, finnkode: str) -> int:
        """
        Remove manual override for a property.
        
        Args:
            finnkode: The property's finn.no code
            
        Returns:
            Number of rows deleted
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM manual_overrides WHERE finnkode = ?', (finnkode,))
        deleted = cursor.rowcount
        
        conn.commit()
        conn.close()
        
        if deleted > 0:
            print(f"✓ Override removed for {finnkode}")
        else:
            print(f"✗ No override found for {finnkode}")
        
        return deleted
    
    def apply_overrides_to_data(self, finnkode: str, data: dict) -> dict:
        """
        Apply overrides to a data dictionary if they exist.
        
        Args:
            finnkode: The property's finn.no code
            data: Dictionary containing property data
            
        Returns:
            Updated data dictionary with overrides applied
        """
        override = self.get_override(finnkode)
        if override:
            if override[0] is not None:  # pris override exists
                data['pris'] = override[0]
            if override[1]:  # adresse override exists
                data['adresse'] = override[1]
            if override[2]:  # postnummer override exists
                data['postnummer'] = override[2]
        
        return data
