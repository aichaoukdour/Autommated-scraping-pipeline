"""
Application Services - Cross-cutting concerns and business logic
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import hashlib
import json

from ..domain.repositories import ChangeDetectionRepository
from ..domain.entities import ScrapedData
from .dto import ChangeDetectionReport


class ScrapingService:
    """Service for scraping-related operations"""
    
    @staticmethod
    def validate_tariff_code(tariff_code: str) -> bool:
        """Validate tariff code format"""
        if not tariff_code:
            return False
        clean_value = ''.join(c for c in tariff_code if c.isdigit())
        return len(clean_value) in [8, 10]


class ChangeDetectionService:
    """Service for detecting website changes"""
    
    def __init__(self, repository: Optional[ChangeDetectionRepository] = None):
        self.repository = repository
        self.previous_log: Optional[Dict[str, Any]] = None
    
    def load_previous_log(self) -> None:
        """Load previous strategy log"""
        if self.repository:
            self.previous_log = self.repository.load_strategy_log()
    
    def check_changes(self, current_log: Dict[str, Any]) -> Optional[ChangeDetectionReport]:
        """Check for changes in scraping strategy"""
        if not self.previous_log:
            return None
        
        if self.repository:
            changes = self.repository.detect_changes(current_log, self.previous_log)
        else:
            changes = self._detect_changes_simple(current_log, self.previous_log)
        
        if not changes:
            return None
        
        return ChangeDetectionReport(
            timestamp=datetime.now(),
            changes=changes,
            previous_timestamp=self.previous_log.get('timestamp') if isinstance(self.previous_log.get('timestamp'), datetime) else None
        )
    
    def detect_and_report(self, current_log: Dict[str, Any]) -> Optional[ChangeDetectionReport]:
        """Detect changes and save current log for next time"""
        report = self.check_changes(current_log)
        
        if self.repository:
            self.repository.save_strategy_log(current_log)
        
        return report
    
    def _detect_changes_simple(self, current: Dict[str, Any], previous: Dict[str, Any]) -> List[str]:
        """Simple change detection without repository"""
        changes = []
        
        # Check search frame strategy
        prev_strategy = previous.get('strategy_log', {}).get('search_frame_strategy')
        curr_strategy = current.get('search_frame_strategy')
        if prev_strategy and curr_strategy and prev_strategy != curr_strategy:
            changes.append(f"Search frame strategy changed: {prev_strategy} → {curr_strategy}")
        
        # Check content frame strategy
        prev_content = previous.get('strategy_log', {}).get('content_frame_strategy')
        curr_content = current.get('content_frame_strategy')
        if prev_content and curr_content and prev_content != curr_content:
            changes.append(f"Content frame strategy changed: {prev_content} → {curr_content}")
        
        # Check frame names
        prev_frames = set(previous.get('frame_names_found', []))
        curr_frames = set(current.get('frame_names', []))
        if prev_frames != curr_frames:
            added = curr_frames - prev_frames
            removed = prev_frames - curr_frames
            if added:
                changes.append(f"New frames detected: {', '.join(added)}")
            if removed:
                changes.append(f"Frames no longer found: {', '.join(removed)}")
        
        return changes


class DataChangeDetectionService:
    """Service for detecting changes in scraped data"""
    
    @staticmethod
    def detect_changes(old_data: Optional[ScrapedData], new_data: ScrapedData) -> Dict[str, Any]:
        """
        Detect changes between two scraped data versions
        
        Returns:
            Dictionary with change information:
            {
                "has_changes": bool,
                "changes": List[Dict],
                "summary": str
            }
        """
        if not old_data:
            return {
                "has_changes": True,
                "changes": [{"type": "created", "field": "all", "message": "New record created"}],
                "summary": "New record"
            }
        
        changes = []
        
        # Compare basic info
        if old_data.basic_info.to_dict() != new_data.basic_info.to_dict():
            changes.append({
                "type": "updated",
                "field": "basic_info",
                "old": old_data.basic_info.to_dict(),
                "new": new_data.basic_info.to_dict()
            })
        
        # Compare sections
        all_sections = set(list(old_data.sections.keys()) + list(new_data.sections.keys()))
        
        for section_name in all_sections:
            old_section = old_data.sections.get(section_name)
            new_section = new_data.sections.get(section_name)
            
            if not old_section and new_section:
                changes.append({
                    "type": "added",
                    "field": f"sections.{section_name}",
                    "new": new_section.to_dict()
                })
            elif old_section and not new_section:
                changes.append({
                    "type": "removed",
                    "field": f"sections.{section_name}",
                    "old": old_section.to_dict()
                })
            elif old_section and new_section:
                old_dict = old_section.to_dict()
                new_dict = new_section.to_dict()
                if old_dict != new_dict:
                    changes.append({
                        "type": "updated",
                        "field": f"sections.{section_name}",
                        "old": old_dict,
                        "new": new_dict
                    })
        
        # Create summary
        if changes:
            change_types = [c["type"] for c in changes]
            summary = f"{len(changes)} change(s): {', '.join(set(change_types))}"
        else:
            summary = "No changes"
        
        return {
            "has_changes": len(changes) > 0,
            "changes": changes,
            "summary": summary
        }
    
    @staticmethod
    def compute_hash(data: ScrapedData) -> str:
        """Compute hash of scraped data for quick comparison"""
        data_json = json.dumps(data.to_dict(), sort_keys=True)
        return hashlib.md5(data_json.encode()).hexdigest()
