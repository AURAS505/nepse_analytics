from django import forms
from .models import PledgeEntrySheet, LoanFacility, MarginRule, LoanInterestHistory

# --- 1. BANK FACILITY FORM ---
class LoanFacilityForm(forms.ModelForm):
    class Meta:
        model = LoanFacility
        fields = ['bank_name', 'account_number', 'sanctioned_limit', 'start_date', 'expiry_date']
        widgets = {
            'start_date': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'expiry_date': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'bank_name': forms.TextInput(attrs={'class': 'form-control'}),
            'account_number': forms.TextInput(attrs={'class': 'form-control'}),
            'sanctioned_limit': forms.NumberInput(attrs={'class': 'form-control'}),
        }

# --- 2. INTEREST RATE FORM ---
class LoanInterestForm(forms.ModelForm):
    class Meta:
        model = LoanInterestHistory
        fields = ['loan_facility', 'effective_date', 'base_rate', 'premium', 'remarks']
        widgets = {
            'effective_date': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'loan_facility': forms.Select(attrs={'class': 'form-select'}),
            'base_rate': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'premium': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'remarks': forms.TextInput(attrs={'class': 'form-control'}),
        }

# --- 3. MARGIN ENTRY FORM ---
class MarginRuleForm(forms.ModelForm):
    class Meta:
        model = MarginRule
        fields = ['loan_facility', 'symbol', 'margin_percent', 'remarks']
        widgets = {
            'loan_facility': forms.Select(attrs={'class': 'form-select'}),
            'symbol': forms.Select(attrs={'class': 'form-select'}),
            'margin_percent': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'remarks': forms.TextInput(attrs={'class': 'form-control'}),
        }

# --- 4. PLEDGE ENTRY SHEET FORM (Exact User Order) ---
class PledgeEntrySheetForm(forms.ModelForm):
    class Meta:
        model = PledgeEntrySheet
        fields = [
            'unique_id',      # 1
            'date',           # 2
            'loan_facility',  # 3 (Bank)
            'demat_account',  # 4 (DP)
            'symbol',         # 5 (Script)
            'action',         # 6
            'kitta',          # 7
            'tx_180_avg',     # 8 (Rendered)
            'tx_closing_price',# 9 (Rendered)
            'tx_margin',      # 10 (Rendered)
            'tx_utilized',    # 12 (Utilized Amt)
            'remarks'
        ]
        # Note: Drawing Amt (11) is calculated, not input.
        
        widgets = {
            'unique_id': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Auto-Generate'}),
            'date': forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
            'loan_facility': forms.Select(attrs={'class': 'form-select', 'id': 'id_loan_facility'}),
            'demat_account': forms.Select(attrs={'class': 'form-select'}),
            'symbol': forms.Select(attrs={'class': 'form-select', 'id': 'id_symbol'}), 
            'action': forms.Select(attrs={'class': 'form-select'}),
            'kitta': forms.NumberInput(attrs={'class': 'form-control'}),
            
            # READONLY Fields (Data populated by JS via API)
            'tx_180_avg': forms.NumberInput(attrs={'class': 'form-control bg-light', 'readonly': True, 'id': 'id_180_avg'}),
            'tx_closing_price': forms.NumberInput(attrs={'class': 'form-control bg-light', 'readonly': True, 'id': 'id_closing_price'}),
            'tx_margin': forms.NumberInput(attrs={'class': 'form-control bg-light', 'readonly': True, 'id': 'id_margin'}),
            
            'tx_utilized': forms.NumberInput(attrs={'class': 'form-control'}),
            'remarks': forms.TextInput(attrs={'class': 'form-control'}),
        }