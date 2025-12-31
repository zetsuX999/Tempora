"""
Tempora Cluster Management Command

Provides CLI interface for managing the Tempora distributed scheduler cluster.

Usage:
    python manage.py tempora_cluster status
    python manage.py tempora_cluster members
    python manage.py tempora_cluster leader
    python manage.py tempora_cluster health
    python manage.py tempora_cluster cleanup

Standard: TEMPORA-HA-001 §9 (Management Commands)
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any, Dict, Optional

from django.core.management.base import BaseCommand, CommandParser
from django.utils import timezone

from tempora.models import (
    ClusterMember,
    ClusterMemberRole,
    ClusterMemberStatus,
)


class Command(BaseCommand):
    """
    Management command for Tempora cluster operations.

    Subcommands:
        status  - Show cluster status summary
        members - List all cluster members
        leader  - Show current cluster leader
        health  - Show health status of all members
        cleanup - Remove stale/failed members
        info    - Show detailed info for a specific member
    """

    help = "Manage Tempora distributed scheduler cluster"

    def add_arguments(self, parser: CommandParser) -> None:
        """Add command arguments."""
        subparsers = parser.add_subparsers(
            dest="subcommand",
            help="Subcommand to run",
        )

        # status subcommand
        status_parser = subparsers.add_parser(
            "status",
            help="Show cluster status summary",
        )
        status_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )

        # members subcommand
        members_parser = subparsers.add_parser(
            "members",
            help="List all cluster members",
        )
        members_parser.add_argument(
            "--status",
            choices=["active", "suspected", "failed", "all"],
            default="all",
            help="Filter by status",
        )
        members_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )

        # leader subcommand
        leader_parser = subparsers.add_parser(
            "leader",
            help="Show current cluster leader",
        )
        leader_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )

        # health subcommand
        health_parser = subparsers.add_parser(
            "health",
            help="Show health status of all members",
        )
        health_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )

        # cleanup subcommand
        cleanup_parser = subparsers.add_parser(
            "cleanup",
            help="Remove stale/failed members",
        )
        cleanup_parser.add_argument(
            "--max-age",
            type=int,
            default=300,
            help="Maximum age in seconds (default: 300)",
        )
        cleanup_parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Show what would be deleted without deleting",
        )

        # info subcommand
        info_parser = subparsers.add_parser(
            "info",
            help="Show detailed info for a specific member",
        )
        info_parser.add_argument(
            "instance_id",
            help="Instance ID to show info for",
        )
        info_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )

        # election subcommand
        election_parser = subparsers.add_parser(
            "election",
            help="Show election state and statistics",
        )
        election_parser.add_argument(
            "--json",
            action="store_true",
            help="Output as JSON",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        """Handle command execution."""
        subcommand = options.get("subcommand")

        if subcommand == "status":
            self._handle_status(options)
        elif subcommand == "members":
            self._handle_members(options)
        elif subcommand == "leader":
            self._handle_leader(options)
        elif subcommand == "health":
            self._handle_health(options)
        elif subcommand == "cleanup":
            self._handle_cleanup(options)
        elif subcommand == "info":
            self._handle_info(options)
        elif subcommand == "election":
            self._handle_election(options)
        else:
            # Default to status
            self._handle_status(options)

    def _handle_status(self, options: Dict[str, Any]) -> None:
        """Show cluster status summary."""
        total = ClusterMember.objects.count()
        active = ClusterMember.objects.filter(
            status=ClusterMemberStatus.ACTIVE
        ).count()
        suspected = ClusterMember.objects.filter(
            status=ClusterMemberStatus.SUSPECTED
        ).count()
        failed = ClusterMember.objects.filter(
            status=ClusterMemberStatus.FAILED
        ).count()

        leader = ClusterMember.get_leader()

        status = {
            "cluster_size": total,
            "active_members": active,
            "suspected_members": suspected,
            "failed_members": failed,
            "has_leader": leader is not None,
            "leader_id": leader.instance_id if leader else None,
            "quorum_available": active >= (total // 2) + 1 if total > 0 else False,
        }

        if options.get("json"):
            self.stdout.write(json.dumps(status, indent=2))
        else:
            self.stdout.write(self.style.SUCCESS("=== Tempora Cluster Status ==="))
            self.stdout.write(f"Cluster Size:     {total}")
            self.stdout.write(f"Active Members:   {active}")
            self.stdout.write(f"Suspected:        {suspected}")
            self.stdout.write(f"Failed:           {failed}")
            self.stdout.write("")

            if leader:
                self.stdout.write(
                    self.style.SUCCESS(f"Leader: {leader.instance_id}")
                )
            else:
                self.stdout.write(
                    self.style.WARNING("Leader: (none)")
                )

            if status["quorum_available"]:
                self.stdout.write(
                    self.style.SUCCESS("Quorum: Available")
                )
            else:
                self.stdout.write(
                    self.style.ERROR("Quorum: NOT AVAILABLE")
                )

    def _handle_members(self, options: Dict[str, Any]) -> None:
        """List all cluster members."""
        status_filter = options.get("status", "all")

        queryset = ClusterMember.objects.all()
        if status_filter != "all":
            queryset = queryset.filter(status=status_filter)

        members = list(queryset.order_by("-role", "instance_id"))

        if options.get("json"):
            data = [m.to_dict() for m in members]
            self.stdout.write(json.dumps(data, indent=2))
        else:
            self.stdout.write(self.style.SUCCESS("=== Cluster Members ==="))
            self.stdout.write("")

            if not members:
                self.stdout.write("(no members)")
                return

            # Header
            self.stdout.write(
                f"{'Instance ID':<20} {'Role':<12} {'Status':<12} {'Host':<20} "
                f"{'Last Heartbeat':<20}"
            )
            self.stdout.write("-" * 90)

            for m in members:
                last_hb = (
                    self._format_time_ago(m.last_heartbeat)
                    if m.last_heartbeat else "never"
                )

                # Color based on status
                if m.status == ClusterMemberStatus.ACTIVE:
                    status_str = self.style.SUCCESS(f"{m.status:<12}")
                elif m.status == ClusterMemberStatus.SUSPECTED:
                    status_str = self.style.WARNING(f"{m.status:<12}")
                else:
                    status_str = self.style.ERROR(f"{m.status:<12}")

                # Color role
                if m.role == ClusterMemberRole.LEADER:
                    role_str = self.style.SUCCESS(f"{m.role:<12}")
                else:
                    role_str = f"{m.role:<12}"

                self.stdout.write(
                    f"{m.instance_id:<20} {role_str} {status_str} "
                    f"{m.host}:{m.port:<13} {last_hb:<20}"
                )

    def _handle_leader(self, options: Dict[str, Any]) -> None:
        """Show current cluster leader."""
        leader = ClusterMember.get_leader()

        if options.get("json"):
            if leader:
                self.stdout.write(json.dumps(leader.to_dict(), indent=2))
            else:
                self.stdout.write(json.dumps({"leader": None}))
        else:
            if leader:
                self.stdout.write(self.style.SUCCESS("=== Cluster Leader ==="))
                self.stdout.write(f"Instance ID:   {leader.instance_id}")
                self.stdout.write(f"Host:          {leader.host}:{leader.port}")
                self.stdout.write(f"Term:          {leader.current_term}")
                self.stdout.write(f"Last Heartbeat: {self._format_time_ago(leader.last_heartbeat)}")
                self.stdout.write(f"Latency:       {leader.heartbeat_latency_ms:.2f} ms" if leader.heartbeat_latency_ms else "Latency:       N/A")
            else:
                self.stdout.write(
                    self.style.WARNING("No leader currently elected")
                )

    def _handle_health(self, options: Dict[str, Any]) -> None:
        """Show health status of all members."""
        members = list(ClusterMember.objects.all().order_by("instance_id"))

        health_data = []
        for m in members:
            health = {
                "instance_id": m.instance_id,
                "status": m.status,
                "consecutive_missed": m.consecutive_missed,
                "seconds_since_heartbeat": m.seconds_since_heartbeat,
                "heartbeat_latency_ms": m.heartbeat_latency_ms,
                "is_healthy": m.is_healthy,
            }
            health_data.append(health)

        if options.get("json"):
            self.stdout.write(json.dumps(health_data, indent=2))
        else:
            self.stdout.write(self.style.SUCCESS("=== Member Health ==="))
            self.stdout.write("")

            if not members:
                self.stdout.write("(no members)")
                return

            self.stdout.write(
                f"{'Instance ID':<20} {'Status':<12} {'Missed':<8} "
                f"{'Since HB':<12} {'Latency':<12}"
            )
            self.stdout.write("-" * 70)

            for h in health_data:
                since_hb = (
                    f"{h['seconds_since_heartbeat']:.1f}s"
                    if h['seconds_since_heartbeat'] else "N/A"
                )
                latency = (
                    f"{h['heartbeat_latency_ms']:.1f}ms"
                    if h['heartbeat_latency_ms'] else "N/A"
                )

                if h["is_healthy"]:
                    status_str = self.style.SUCCESS(f"{h['status']:<12}")
                else:
                    status_str = self.style.ERROR(f"{h['status']:<12}")

                self.stdout.write(
                    f"{h['instance_id']:<20} {status_str} "
                    f"{h['consecutive_missed']:<8} {since_hb:<12} {latency:<12}"
                )

    def _handle_cleanup(self, options: Dict[str, Any]) -> None:
        """Remove stale/failed members."""
        max_age = options.get("max_age", 300)
        dry_run = options.get("dry_run", False)

        cutoff = timezone.now() - timedelta(seconds=max_age)

        stale_members = ClusterMember.objects.filter(
            last_heartbeat__lt=cutoff,
            status__in=[
                ClusterMemberStatus.FAILED,
                ClusterMemberStatus.SUSPECTED,
            ],
        )

        count = stale_members.count()

        if dry_run:
            self.stdout.write(
                f"Would delete {count} stale members:"
            )
            for m in stale_members:
                self.stdout.write(f"  - {m.instance_id} (last seen: {m.last_heartbeat})")
        else:
            if count > 0:
                stale_members.delete()
                self.stdout.write(
                    self.style.SUCCESS(f"Deleted {count} stale members")
                )
            else:
                self.stdout.write("No stale members to clean up")

    def _handle_info(self, options: Dict[str, Any]) -> None:
        """Show detailed info for a specific member."""
        instance_id = options.get("instance_id")

        try:
            member = ClusterMember.objects.get(instance_id=instance_id)
        except ClusterMember.DoesNotExist:
            self.stdout.write(
                self.style.ERROR(f"Member not found: {instance_id}")
            )
            return

        if options.get("json"):
            self.stdout.write(json.dumps(member.to_dict(), indent=2))
        else:
            self.stdout.write(self.style.SUCCESS(f"=== Member: {member.instance_id} ==="))
            self.stdout.write("")
            self.stdout.write(f"Instance ID:      {member.instance_id}")
            self.stdout.write(f"Host:             {member.host}:{member.port}")
            self.stdout.write(f"Role:             {member.role}")
            self.stdout.write(f"Status:           {member.status}")
            self.stdout.write(f"Current Term:     {member.current_term}")
            self.stdout.write(f"Voted For:        {member.voted_for or '(none)'}")
            self.stdout.write("")
            self.stdout.write(f"Last Heartbeat:   {member.last_heartbeat or 'never'}")
            self.stdout.write(f"Missed Beats:     {member.consecutive_missed}")
            self.stdout.write(f"Latency:          {member.heartbeat_latency_ms:.2f} ms" if member.heartbeat_latency_ms else "Latency:          N/A")
            self.stdout.write("")
            self.stdout.write(f"Joined At:        {member.joined_at}")
            self.stdout.write(f"Is Healthy:       {member.is_healthy}")
            self.stdout.write(f"Is Leader:        {member.is_leader}")

            if member.metadata:
                self.stdout.write("")
                self.stdout.write("Metadata:")
                for k, v in member.metadata.items():
                    self.stdout.write(f"  {k}: {v}")

    def _format_time_ago(self, dt: Optional[Any]) -> str:
        """Format a datetime as time ago string."""
        if not dt:
            return "never"

        now = timezone.now()
        diff = now - dt

        if diff.total_seconds() < 60:
            return f"{int(diff.total_seconds())}s ago"
        elif diff.total_seconds() < 3600:
            return f"{int(diff.total_seconds() / 60)}m ago"
        elif diff.total_seconds() < 86400:
            return f"{int(diff.total_seconds() / 3600)}h ago"
        else:
            return f"{int(diff.total_seconds() / 86400)}d ago"

    def _handle_election(self, options: Dict[str, Any]) -> None:
        """Show election state and statistics."""
        from django.db.models import Max, Count

        # Get current term from all members
        term_info = ClusterMember.objects.aggregate(
            max_term=Max("current_term"),
            member_count=Count("id"),
        )

        # Get leader
        leader = ClusterMember.get_leader()

        # Get vote distribution
        vote_counts = {}
        members = list(ClusterMember.objects.all())
        for m in members:
            if m.voted_for:
                vote_counts[m.voted_for] = vote_counts.get(m.voted_for, 0) + 1

        # Get role distribution
        role_counts = {}
        for m in members:
            role_counts[m.role] = role_counts.get(m.role, 0) + 1

        # Calculate quorum
        total = term_info["member_count"] or 0
        quorum_size = (total // 2) + 1 if total > 0 else 1
        active_count = role_counts.get(ClusterMemberStatus.ACTIVE, 0)

        election_data = {
            "current_term": term_info["max_term"] or 0,
            "total_members": total,
            "quorum_size": quorum_size,
            "has_quorum": active_count >= quorum_size,
            "leader": leader.instance_id if leader else None,
            "leader_term": leader.current_term if leader else None,
            "role_distribution": role_counts,
            "vote_distribution": vote_counts,
            "members": [
                {
                    "instance_id": m.instance_id,
                    "role": m.role,
                    "term": m.current_term,
                    "voted_for": m.voted_for,
                    "status": m.status,
                }
                for m in members
            ],
        }

        if options.get("json"):
            self.stdout.write(json.dumps(election_data, indent=2))
        else:
            self.stdout.write(self.style.SUCCESS("=== Election State ==="))
            self.stdout.write("")

            # Term info
            self.stdout.write(f"Current Term:   {election_data['current_term']}")
            self.stdout.write(f"Total Members:  {total}")
            self.stdout.write(f"Quorum Size:    {quorum_size}")
            self.stdout.write("")

            # Quorum status
            if election_data["has_quorum"]:
                self.stdout.write(self.style.SUCCESS("Quorum: AVAILABLE"))
            else:
                self.stdout.write(self.style.ERROR("Quorum: NOT AVAILABLE"))
            self.stdout.write("")

            # Leader info
            if leader:
                self.stdout.write(self.style.SUCCESS(f"Leader: {leader.instance_id}"))
                self.stdout.write(f"  Term: {leader.current_term}")
                self.stdout.write(f"  Host: {leader.host}:{leader.port}")
            else:
                self.stdout.write(self.style.WARNING("Leader: (none elected)"))
            self.stdout.write("")

            # Role distribution
            self.stdout.write("Role Distribution:")
            for role, count in sorted(role_counts.items()):
                if role == ClusterMemberRole.LEADER:
                    self.stdout.write(self.style.SUCCESS(f"  {role}: {count}"))
                else:
                    self.stdout.write(f"  {role}: {count}")
            self.stdout.write("")

            # Vote distribution
            if vote_counts:
                self.stdout.write("Vote Distribution (current term):")
                for candidate, votes in sorted(
                    vote_counts.items(), key=lambda x: -x[1]
                ):
                    if candidate == leader.instance_id if leader else None:
                        self.stdout.write(
                            self.style.SUCCESS(f"  {candidate}: {votes} votes")
                        )
                    else:
                        self.stdout.write(f"  {candidate}: {votes} votes")
            else:
                self.stdout.write("Vote Distribution: (no votes recorded)")
            self.stdout.write("")

            # Member table
            self.stdout.write("Member Election State:")
            self.stdout.write(
                f"{'Instance ID':<20} {'Role':<12} {'Term':<8} {'Voted For':<20}"
            )
            self.stdout.write("-" * 65)

            for m in sorted(members, key=lambda x: (-x.current_term, x.instance_id)):
                role_str = m.role
                if m.role == ClusterMemberRole.LEADER:
                    role_str = self.style.SUCCESS(f"{m.role:<12}")
                elif m.role == ClusterMemberRole.CANDIDATE:
                    role_str = self.style.WARNING(f"{m.role:<12}")
                else:
                    role_str = f"{m.role:<12}"

                voted_for = m.voted_for or "(none)"

                self.stdout.write(
                    f"{m.instance_id:<20} {role_str} {m.current_term:<8} {voted_for:<20}"
                )
