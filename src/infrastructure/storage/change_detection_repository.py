"""
Change detection repository implementation
"""

import json
import os
from typing import Optional, List, Dict, Any
from datetime import datetime

from ...domain.repositories import ChangeDetectionRepository


class FileChangeDetectionRepository(ChangeDetectionRepository):
    """File-based change detection repository"""
    
    def __init__(self, log_file: str = 'scraper_run_log.json'):
        self.log_file = log_file
    
    def save_strategy_log(self, strategy_log: dict) -> bool:
        """Save strategy log to file"""
        try:
            run_data = {
                'timestamp': datetime.now().isoformat(),
                'strategy_log': strategy_log.get('strategy_log', strategy_log),
                'base_url': strategy_log.get('base_url', 'https://www.douane.gov.ma/adil/'),
                'frame_names_found': list(set(strategy_log.get('frame_names', []))),
                'selectors_used': list(set(strategy_log.get('selectors_used', [])))
            }
            with open(self.log_file, 'w', encoding='utf-8') as f:
                json.dump(run_data, f, indent=2, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error saving strategy log: {e}")
            return False
    
    def load_strategy_log(self) -> Optional[dict]:
        """Load previous strategy log from file"""
        try:
            if not os.path.exists(self.log_file):
                return None
            
            with open(self.log_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading strategy log: {e}")
            return None
    
    def detect_changes(self, current_log: dict, previous_log: dict) -> List[str]:
        """Detect changes between current and previous logs"""
        changes = []
        
        # Ensure we're comparing the right structure
        current_strategy_log = current_log.get('strategy_log', current_log)
        previous_strategy_log = previous_log.get('strategy_log', previous_log)
        
        # Check search frame strategy
        prev_strategy = previous_strategy_log.get('search_frame_strategy')
        curr_strategy = current_strategy_log.get('search_frame_strategy')
        if prev_strategy and curr_strategy and prev_strategy != curr_strategy:
            changes.append(f"Search frame strategy changed: {prev_strategy} → {curr_strategy}")
        
        # Check content frame strategy
        prev_content = previous_strategy_log.get('content_frame_strategy')
        curr_content = current_strategy_log.get('content_frame_strategy')
        if prev_content and curr_content and prev_content != curr_content:
            changes.append(f"Content frame strategy changed: {prev_content} → {curr_content}")
        
        # Check frame names
        prev_frames = set(previous_log.get('frame_names_found', previous_strategy_log.get('frame_names', [])))
        curr_frames = set(current_log.get('frame_names', current_strategy_log.get('frame_names', [])))
        if prev_frames != curr_frames:
            added = curr_frames - prev_frames
            removed = prev_frames - curr_frames
            if added:
                changes.append(f"New frames detected: {', '.join(added)}")
            if removed:
                changes.append(f"Frames no longer found: {', '.join(removed)}")
        
        # Check selectors used
        prev_selectors = set(previous_log.get('selectors_used', previous_strategy_log.get('selectors_used', [])))
        curr_selectors = set(current_log.get('selectors_used', current_strategy_log.get('selectors_used', [])))
        if prev_selectors != curr_selectors:
            new_selectors = curr_selectors - prev_selectors
            old_selectors = prev_selectors - curr_selectors
            if new_selectors:
                changes.append(f"New selectors being used: {', '.join(list(new_selectors)[:5])}")
            if old_selectors:
                changes.append(f"Old selectors no longer work: {', '.join(list(old_selectors)[:5])}")
        
        # Check base URL
        prev_url = previous_log.get('base_url')
        curr_url = current_log.get('base_url')
        if prev_url and curr_url and prev_url != curr_url:
            changes.append(f"Base URL changed: {prev_url} → {curr_url}")
        
        return changes

