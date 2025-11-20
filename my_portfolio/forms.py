# my_portfolio/forms.py
from django import forms
from .models import DematAccount
from django.utils import timezone

class DematAccountForm(forms.ModelForm):
    class Meta:
        model = DematAccount
        fields = ['capital_name', 'demat_number', 'owner_name']
        widgets = {
            'capital_name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'e.g. Nabil Investment'}),
            'demat_number': forms.TextInput(attrs={'class': 'form-control', 'placeholder': '16-digit BOID'}),
            'owner_name': forms.TextInput(attrs={'class': 'form-control', 'placeholder': 'Account Holder Name'}),
        }

class MeroShareUploadForm(forms.Form):
    demat_account = forms.ModelChoiceField(
        queryset=DematAccount.objects.all(),
        widget=forms.Select(attrs={'class': 'form-select'}),
        label="Select Demat Account"
    )
    # --- NEW DATE FIELD ---
    snapshot_date = forms.DateField(
        initial=timezone.now().date(),
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
        label="Snapshot Date (For Storage)"
    )
    file = forms.FileField(
        widget=forms.FileInput(attrs={'class': 'form-control'}),
        label="Upload 'My Share' CSV"
    )

class TransferRequestUploadForm(forms.Form):
    demat_account = forms.ModelChoiceField(
        queryset=DematAccount.objects.all(),
        widget=forms.Select(attrs={'class': 'form-select'}),
        label="Select Demat Account"
    )
    # --- NEW DATE FIELD ---
    settlement_date = forms.DateField(
        initial=timezone.now().date(),
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),
        label="Apply Reduction to Date"
    )
    file = forms.FileField(
        widget=forms.FileInput(attrs={'class': 'form-control'}),
        label="Upload 'Transfer Request' CSV"
    )