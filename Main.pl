use DBI;
use Digest::MD5;
use POSIX qw(strftime);
use Getopt::Std;

# Global vars
our $nodb; # Object used to store the connection to the DB
our %config; # Configuration loaded from "Config.pl" (Hardcoded for the moment)
our %opts;

# Declare the subroutine that will be used below.
sub Main; # Will be called in every situation
sub Init; # Will be used in case we need to create the initial setup (db mainly)
sub printHelp; # Will print help in case user send bad options
sub Normal; # Will be used in normal running (Init already done)
sub Normal1Dir; # Is used by Normal to do the jobs for one dir
sub Normal1File; # Is used by Normal1Dir to work on a specific file
sub InsertFile; # Is used to handle a new file
sub InsertFileIntoDB; # Is used to insert a file in the DB (usually called by InsertFile)
sub UpdateFile; # Is used to update a file
sub UpdateFileInDB; # Is used to update an entry each time the checksum is modified  (usually called by UpdateFile)

getopts('n', \%opts) or printHelp;
Main;

sub Init () {
	my @dbCreationStatements;

	$dbCreationStatements[0]="";
	$dbCreationStatements[1]="CREATE TABLE FILES (ID_File integer primary key autoincrement, ID_Directory integer, Filename text, HASH varchar(32), ID_Backup integer);";
	$dbCreationStatements[2]="CREATE TABLE BACKUPS (ID_Backup integer primary key autoincrement, Date text, Comment text);";
	$dbCreationStatements[3]="CREATE TABLE DIRECTORIES (ID_Dir integer primary key autoincrement, DirName text);";

	my $req;
	while (defined ($req = shift @dbCreationStatements)) {
		my $link = $nodb->prepare($dbCreationStatements[0]);
		$link->execute();
	}
	
}

sub Normal {
	my $idBackup;

	# Initiate a new backup : Create a new one in DB
	my $bakDate = strftime "%Y/%m/%d %I:%M:%S %p", localtime;
	my $dbReqNewBackup = "INSERT INTO BACKUPS('Date') VALUES('".$bakDate."');";
	my $linkNewBackup = $nodb->prepare($dbReqNewBackup);
	$linkNewBackup->execute;

	# Get Current ID_Backup
	my $dbReqGetIdBackup = "SELECT seq FROM sqlite_sequence WHERE name='BACKUPS';";
	my $linkGetIdBackup = $nodb->prepare($dbReqGetIdBackup);
	$linkGetIdBackup->execute;
	$idBackup = $linkGetIdBackup->fetch->[0];

	# RUN the operations
	while (defined (my $lDir = shift $config{srcdir})) {
		Normal1Dir $lDir, $idBackup;
		$idDir++;
	}
}

sub Normal1Dir  {
	my $wDir = $_[0]; # Argument 1 is SourceDirectory
	my $wIdBackup = $_[1]; # Argument 2 is Backup ID

	my $wIdDir; # Should be initialized by checking in DB. To be corrected

	# Insert if not present the directory to backup
	my $dbCheckDir = "SELECT ID_Dir FROM DIRECTORIES WHERE DirName='".$wDir."';";
	my $linkCheckDir = $nodb->prepare($dbCheckDir);
	$linkCheckDir->execute;
	if (! defined (my $rowCheckDir = $linkCheckDir->fetch)) {
		# INSERT INTO DB
		my $dbInsertDir = "INSERT INTO DIRECTORIES('DirName') VALUES('".$wDir."');";
		my $linkInsertDir = $nodb->prepare($dbInsertDir);
		$linkInsertDir->execute;
		my $linkReCheckDir = $nodb->prepare($dbCheckDir);
		$linkReCheckDir->execute;
		$wIdDir=$linkReCheckDir->fetch->[0];
		print ""
	} else {
		$wIdDir= $rowCheckDir->[0];
	}
	
	chdir($wDir);
	my %listfiles = `find . -type f -print`;
	
	foreach my $v (values(%listfiles)) {
		chomp($v);
		stat($v);
		if (-f _ && ! -z _) {
			Normal1File $v, $wDir, $wIdBackup, $wIdDir;
		} else {
			print "NO : '$v' will not be backed up.\n";
		}
	}
}

sub Normal1File {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $wIdBackup = $_[2]; # Argument 3 is Backup ID
	my $wIdDir = $_[3]; # Argument 4 is Directory ID

	open (FILE, $filename);
	binmode(FILE);
	my $md5Checksum	= Digest::MD5->new;
	while (<FILE>) {
		$md5Checksum->add($_);
	}
	close FILE;

	# Test if file is already in the table
	my $dbReqLookFor = "SELECT HASH from FILES where Filename='".$filename."' AND ID_Directory = ".$wIdDir.";";
	my $linkReqLookFor = $nodb->prepare($dbReqLookFor);
	$linkReqLookFor->execute;
	if (my $rowReqLookFor = $linkReqLookFor->fetch) {
		my $tHash = $rowReqLookFor->[0]; 
		if ($md5Checksum->hexdigest != $tHash) {
			UpdateFile $filename, $wDir, $wIdBackup, $md5Checksum->hexdigest;
		} else {
			#print "Same Checkshum. \n";			
		}
	} else {
		#print "NEW : '$filename is new. Inserting in DB.\n";
		InsertFile $filename, $wDir, $wIdBackup, $wIdDir, $md5Checksum->hexdigest;
	}
}

sub InsertFile {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $wIdBackup = $_[2]; # Argument 3 is Backup ID
	my $wIdDir = $_[3]; # Argument 4 is Directory ID
	my $wChecksum = $_[4]; # Argument 5 is Checksum

	InsertFileIntoDB $filename, $wDir, $wIdBackup, $wIdDir, $wChecksum;
}

sub InsertFileIntoDB {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $wIdBackup = $_[2]; # Argument 3 is Backup ID
	my $wIdDir = $_[3]; # Argument 4 is Directory ID
	my $wChecksum = $_[4]; # Argument 5 is Checksum

	# Insert into DB as the file is not present at the moment
	my $dbReqInsert = "INSERT INTO FILES('ID_Directory', 'Filename', 'HASH', 'ID_Backup') VALUES(".$wIdDir.", \"".$filename."\", \"".$wChecksum."\", ".$wIdBackup.");";
	my $linkInsert = $nodb->prepare($dbReqInsert);
	$linkInsert->execute;
	
}

sub UpdateFile{
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $hexMD5 = $_[2]; # Argument 3 is MD5 Checksum

	# Create a new encrypted file
	
	# Remove the previous one

	# Finally update Database
	UpdateFileInDB $filename, $wDir, $hexMD5;
}

sub UpdateFileInDB {
	my $filename = $_[0]; # Argument 1 is Filename
	my $wDir = $_[1]; # Argument 2 is SourceDirectory
	my $hexMD5 = $_[2]; # Argument 3 is MD5 Checksum

	my $dbReqUpdate = "UPDATE FILES SET HASH='".$hexMD5."' WHERE Filename='".$filename."' AND ID_Directory='".$wDir."';";
	my $linkUpdate = $nodb->prepare($dbReqUpdate);
	$linkUpdate->execute;
}

sub Main {
	
	%config = do "Config.pl";

	# Initialize DB Connection
	$nodb = DBI->connect("dbi:SQLite:dbname=".$config{sqldb},"","");
	
	
	if (defined $opts{'n'}) {
		Init;
	}
	
	Normal;
	$nodb->disconnect;
}

sub printHelp {
	print "Only use -n to init DB\n";
	die;
}
