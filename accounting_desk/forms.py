from django import forms
from .models import LoanFacility, PledgedScrip, LoanInterestHistory
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
            'base_rate': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01', 'id': 'id_base_rate', 'placeholder': 'Base Rate %'}),
            'premium': forms.NumberInput(attrs={'class': 'form-control', 'step': '0.01', 'id': 'id_premium', 'placeholder': 'Premium %'}),
            'effective_date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'end_date': forms.DateInput(attrs={'class': 'form-control', 'type': 'date'}),
            'remarks': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g. Q2 Base Rate Hike'}),
        }
class PledgedScripForm(forms.ModelForm):
    # NEW FIELD: Boolean checkbox to bypass free_balance check
    record_existing_pledge = forms.BooleanField(
        required=False, 
        label="Record Existing Pledge",
        help_text="Check this if these shares are ALREADY pledged in your Demat/MeroShare."
    )

    class Meta:
        model = PledgedScrip
        fields = [
            'loan_facility', 'demat_account', 'symbol', 
            'quantity', 'average_price', 'average_price_days', 'valuation_percent'
        ]
        widgets = {
            'loan_facility': forms.Select(attrs={'class': 'form-select'}),
            'demat_account': forms.Select(attrs={'class': 'form-select', 'id': 'id_demat_account'}),
            'symbol': forms.TextInput(attrs={'class': 'form-control', 'id': 'id_symbol', 'placeholder': 'e.g. NICA'}),
            'quantity': forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_quantity'}),
            'average_price': forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_avg_price'}),
            'average_price_days': forms.NumberInput(attrs={'class': 'form-control', 'value': 180}),
            'valuation_percent': forms.NumberInput(attrs={'class': 'form-control', 'id': 'id_val_percent', 'value': 50}),
        }

    def clean(self):
        cleaned_data = super().clean()
        demat = cleaned_data.get('demat_account')
        symbol_ticker = cleaned_data.get('symbol')
        quantity = cleaned_data.get('quantity')
        is_existing = cleaned_data.get('record_existing_pledge') # Get checkbox value

        if demat and symbol_ticker and quantity:
            # Fetch latest holding snapshot
            holding = MeroShareHolding.objects.filter(
                demat_account=demat, 
                symbol__script_ticker=symbol_ticker
            ).order_by('-snapshot_date').first()

            if not holding:
                raise forms.ValidationError(f"No holdings found for {symbol_ticker} in {demat.capital_name}")
            
            # --- CONDITIONAL VALIDATION ---
            if is_existing:
                # Logic: If recording existing, check if we have enough PLEDGED balance
                if quantity > holding.pledge_balance:
                    raise forms.ValidationError(
                        f"Mismatch! You only have {holding.pledge_balance} shares marked as 'Pledged' in MeroShare, but you are trying to record {quantity}."
                    )
            else:
                # Logic: New pledge requires FREE balance
                if quantity > holding.free_balance:
                    raise forms.ValidationError(
                        f"Insufficient Free Balance! Available: {holding.free_balance}, Requested: {quantity}. (If already pledged, check 'Record Existing Pledge')"
                    )

        return cleaned_data