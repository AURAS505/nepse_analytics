from django import forms
from .models import LoanFacility, PledgedScrip, LoanInterestHistory, PledgeEntry, StockMargin
from my_portfolio.models import MeroShareHolding, DematAccount

class LoanFacilityForm(forms.ModelForm):
    class Meta:
        model = LoanFacility
        fields = ['bank_name', 'account_number', 'sanctioned_limit', 'start_date', 'expiry_date']
        widgets = {
            'bank_name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g. Nabil Bank'}),
            'account_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Loan A/C No.'}),
            'sanctioned_limit': forms.NumberInput(attrs={'class': 'form-control'}),
            'start_date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'expiry_date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
        }

class LoanInterestForm(forms.ModelForm):
    class Meta:
        model = LoanInterestHistory
        fields = ['loan_facility', 'base_rate', 'premium', 'effective_date', 'end_date', 'remarks']
        widgets = {
            'loan_facility': forms.Select(attrs={'class': 'form-select'}),
            'base_rate': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'premium': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01'}),
            'effective_date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'end_date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'remarks': forms.TextInput(attrs={'class': 'form-control'}),
        }

class StockMarginForm(forms.ModelForm):
    class Meta:
        model = StockMargin
        fields = ['date', 'loan_facility', 'script', 'margin', 'remarks']
        widgets = {
            'date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'loan_facility': forms.Select(attrs={'class': 'form-select'}),
            'script': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g. NICA'}),
            'margin': forms.NumberInput(attrs={'class': 'form-control', 'placeholder': '50'}),
            'remarks': forms.TextInput(attrs={'class': 'form-control'}),
        }

class PledgeEntryForm(forms.ModelForm):
    # Explicitly add closing_price to allow editing for Balance b/d
    closing_price = forms.DecimalField(
        required=False, 
        widget=forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_closing_price', 'readonly': 'readonly'})
    )

    class Meta:
        model = PledgeEntry
        fields = [
            'date', 'loan_facility', 'demat_account', 'symbol', 
            'action', 'margin', 'kitta', 
            'average_closing_price', 'closing_price', 'utilized_loan'
        ]
        widgets = {
            'date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'loan_facility': forms.Select(attrs={'class': 'form-select', 'id': 'id_loan_facility'}),
            'demat_account': forms.Select(attrs={'class': 'form-select', 'id': 'id_demat'}),
            'symbol': forms.TextInput(attrs={'class': 'form-control', 'id': 'id_symbol'}),
            'action': forms.Select(attrs={'class': 'form-select', 'id': 'id_action'}),
            'margin': forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_margin'}),
            'kitta': forms.NumberInput(attrs={'class': 'form-control'}),
            'average_closing_price': forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_avg_price'}),
            'utilized_loan': forms.NumberInput(attrs={'class': 'form-control'}),
        }