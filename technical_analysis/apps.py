from django.apps import AppConfig


class TechnicalAnalysisConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'technical_analysis'
    verbose_name = 'Technical Analysis'
    
    def ready(self):
        """Import signal handlers when app is ready"""
        # You can import signals here if needed
        # import technical_analysis.signals
        pass